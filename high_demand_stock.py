import io
import re
import logging
from typing import Set, Dict
import boto3
from botocore.config import Config
import pandas as pd
from apps.brand.enums import SubscriptionType
from apps.product_v2.models.product import Product
from apps.stock.models import StockExtraInStockRequest
from apps.stock.queries.high_demand_stock import HIGH_DEMAND_STOCK
from apps.brand.models import Brand
from django.utils import timezone
from django.core.mail import EmailMessage
from django.conf import settings
from django.template.loader import render_to_string
from django.db.models import Q
from utils.date import start_of_today, last_n_weeks_from_now, last_n_months_from_now
from utils.athena.query_execution import execute_athena_query
from utils.pandas.export_dataframe import ExportDataframeWrapper
from utils.slack.client import post_files, post_message
from tqdm import tqdm


class StockDataframeWrapper():
    def __init__(self, df: pd.DataFrame):
        self.df = df
        
    def filter_idle_stock(self, in_stock_count: int):
        """유휴재고 in_stock_count개 이하"""
        self.df = self.df.loc[self.df["current_in_stock_count"]<=in_stock_count]
        return self
    
    def filter_view_count(self, view_count: int):
        """1주일간 조회수 view_count개 이상"""
        self.df = self.df.loc[self.df["view_count"]>view_count]
        return self
        
    def filter_like_count(self, like_count: int):
        """전체 like 수 like_count개 이상"""      
        self.df = self.df.loc[self.df["accumulative_product_like_count"]>like_count]
        return self
    
    def filter_last_tryset_time(self, last_tryset_item_created_time: str):
        """마지막 tryset 이용 날짜 last_tryset_item_created_time 이내"""
        self.df = self.df.loc[self.df["latest_tryset_item_created_time"]>last_tryset_item_created_time]
        return self    
    
    def filter_adequate_stock_count(self):
        """적정 재고수 보다 현재 재고 수가 적은 숫자"""
        self.df = self.df.loc[self.df["adquate_stock_count"]>self.df["stock_count"]]
        return self
        
    def filter_look_upload_rate(
        self, 
        look_upload_rate: float, 
        look_upload_rate_over_time: str, 
        first_in_stock_time: str
    ):
        """(재고 입고일 이후 3달 이상 && 게시율 40% 이상) OR 재고 입고일 이후 1주일 이상"""
        self.df = self.df.loc[
            (self.df['first_in_stock_time']!=None) 
            & (
                (
                    (self.df["look_post_rate"]>look_upload_rate) 
                    & (self.df['first_in_stock_time']<=look_upload_rate_over_time)
                ) | 
                (
                    (self.df['first_in_stock_time']<first_in_stock_time) 
                    & (self.df['first_in_stock_time']>look_upload_rate_over_time)    
                )
            )
        ]
        return self
        
    def sort(self, key: str, ascending: bool):
        self.df = self.df.sort_values(by=[key], ascending=ascending)
        

class HighDemandStockFilter():
    IN_STOCK_COUNT=1
    VIEW_COUNT=20
    LIKE_COUNT=15
    ADEQUATE_STOCK_COUNT=[15,10,5]
    LOOK_POST_RATE=40.0
    
    def __init__(self, dataframe: pd.DataFrame):
        self.dataframe = dataframe.copy()
        self.LAST_TRYSET_ITEM_CREATED_TIME = ""
        self.COLD_START_PRODUCT_TIME = ""
        self.sent_mail_product = set()

    def __check_product_requested(self, row) -> bool:
        return int(row["product_id"]) in self.sent_mail_product

    def __set_adequate_stock_count(self, row) -> int:
        """적정 재고수
        
        15개: 1주일간 조회수 100 이상
        
        10개: 1주일간 조회수 50이상 100미만, 누적 좋아요 수 20이상
        
        5개: 나머지
        """
        if row["view_count"] > 100:
            return self.ADEQUATE_STOCK_COUNT[0]
        elif row["view_count"] > 50 and row["accumulative_product_like_count"] > 20:
            return self.ADEQUATE_STOCK_COUNT[1]
        else:
            return self.ADEQUATE_STOCK_COUNT[2]
        
    def __set_look_post_rate(self, row) -> float:
        return (row["look_count"] / row["tryset_item_count"]) * 100
        
    def filter_high_demand_stock(self):
        self.LAST_TRYSET_ITEM_CREATED_TIME = str(last_n_months_from_now(n=1))
        self.COLD_START_PRODUCT_TIME= str(last_n_months_from_now(n=3))
        self.FIRST_IN_STOCK_TIME = str(last_n_weeks_from_now(n=1))
        
        self.sent_mail_product = set(StockExtraInStockRequest.objects \
            .values_list("product", flat=True).distinct("product"))
        self.dataframe["adquate_stock_count"] = self.dataframe.apply(lambda row: self.__set_adequate_stock_count(row), axis=1)
        self.dataframe["requested_mail"] = self.dataframe.apply(lambda row: self.__check_product_requested(row), axis=1)
        self.dataframe["look_post_rate"] = self.dataframe.apply(lambda row: self.__set_look_post_rate(row), axis=1)
        
        stock_data = StockDataframeWrapper(df=self.dataframe)
        stock_data.filter_idle_stock(self.IN_STOCK_COUNT) \
            .filter_view_count(self.VIEW_COUNT) \
            .filter_like_count(self.LIKE_COUNT) \
            .filter_last_tryset_time(self.LAST_TRYSET_ITEM_CREATED_TIME) \
            .filter_adequate_stock_count() \
            .filter_look_upload_rate(self.LOOK_POST_RATE, self.COLD_START_PRODUCT_TIME, self.FIRST_IN_STOCK_TIME) \
            .sort(key="view_count", ascending=False)
        
        self.dataframe = stock_data.df
    
    def get_filtered_items(self) -> pd.DataFrame:
        return self.dataframe.filter(items=[
            "product_id",
            "product_name",
            "brand_id",
            "brand_name",
            "manager_page_url",
            "adquate_stock_count",
            "current_in_stock_count",
            "current_in_use_count",
            "accumulative_product_like_count",
            "view_count",
            "requested_mail",
            "first_in_stock_time",
        ]).rename(columns={
            "product_name": "상품명",
            "brand_name": "브랜드",
            "manager_page_url": "매니저 페이지 링크",
            "adquate_stock_count": "적정 재고수",
            "current_in_stock_count": "입고중 재고",
            "current_in_use_count": "이용중 재고",
            "accumulative_product_like_count": "누적 좋아요 수",
            "view_count": "1주일간 조회수",
            "requested_mail": "3주내 요청이력",
            "first_in_stock_time": "재고 입고일",
        })


class HighDemandStockNudge():
    MAIL_CYCLE_WEEKS=3
    
    def __init__(self, high_demand_stock: pd.DataFrame):
        self.high_demand_stock = high_demand_stock.copy()
        self.filtered_brands = set()
        self.prepared_instock_request_rows = []
        self.mail_list = dict()  # 메일 보낼 리스트 { "브랜드 ID": 프로덕트 Dataframe }
    
    def __filter_brands(self, weeks: int) -> Set[int]:
        """weeks(int)이내에 메일을 보냈던 브랜드를 반환"""
        cycle = last_n_weeks_from_now(n=weeks)
        
        return set(StockExtraInStockRequest.objects.filter(
            ~Q(product__brand__subscription_type=SubscriptionType.ENTERPRISE) | 
            Q(requested_time__gte=cycle),
        ).values_list("product__brand", flat=True).distinct("product__brand"))
    
    def get_high_demand_product(self) -> Dict[int, pd.DataFrame]:
        """메일 보낼 내용 보관"""
        self.filtered_brands = self.__filter_brands(self.MAIL_CYCLE_WEEKS)
        grouped_product = self.high_demand_stock.groupby("brand_id")
        for brand_id, products in grouped_product:
            if int(brand_id) not in self.filtered_brands:
                self.mail_list[int(brand_id)] = products.loc[products["3주내 요청이력"] == False]

        return self.mail_list
    
    def save_instock_request(self, df: pd.DataFrame):
        prepared_instock_request_rows = [
            StockExtraInStockRequest(
                product=Product.objects.get(id=rows["product_id"]),
                round=0,
                requested_time=timezone.now(),
                request_amount=rows["적정 재고수"],
            ) for _, rows in df.iterrows()
        ]
        
        StockExtraInStockRequest.objects.bulk_create(prepared_instock_request_rows)
        
        
logger = logging.getLogger(__name__)
    
class TaskStockNudging():
    REGION_NAME="ap-northeast-2"

    BUCKET_NAME="high-demand-stock-data"
    ROOT_FOLDER_NAME="high_demand_product"
    DATABASE="amplitude"
    
    def __get_s3_resource(self):
        return boto3.resource(
            "s3",
            region_name=self.REGION_NAME,
            config=Config(signature_version="s3v4"),
        )
        
    def __get_athena_client(self):
        return boto3.client("athena", region_name=self.REGION_NAME)
    
    def __get_s3_data(self, bucket_name, file_location):
        s3 = self.__get_s3_resource()
        
        bucket = s3.Bucket(name=bucket_name)
        return bucket.Object(file_location).get()["Body"].read()
    
    def __read_csv_data(self, data: bytes):
        return pd.read_csv(io.BytesIO(data))
    
    def __prepare_query_paramters(self):
        """
        Parameters:
        [
            datetime(str): view_count 기간, 
            datetime(str): add_to_closet 기간, 
            datetime(str): like_count 기간,
        ]
        """
        last_week = str(last_n_weeks_from_now(n=1))
        return [last_week, last_week, last_week]
    
    def __export_dataframe(self, df: pd.DataFrame, for_mail: bool=False):
        if not df.shape[0]:
            return
        
        df_wrapper = ExportDataframeWrapper(dataframe=df, sheet_name="High Demand Stock")
        df_wrapper.set_style_properties(
            {
                "border-left": "2px solid red",
                "border-right": "2px solid red",
            },
            subset=["적정 재고수"]
        )
        last_index = df["적정 재고수"].index[-1]
        df_wrapper.set_style_properties(
            {
                "border-bottom": "2px solid red",
            },
            subset=([last_index], "적정 재고수")
        )
        df_wrapper.export_dataframe_to_excel(index=False)
        df_wrapper.set_header_format(
            format={
                "bold": True,
                "fg_color": "#cfe1f3",
                "border": 1
            }, columns={
                "적정 재고수": {
                    "bold": True,
                    "fg_color": "#fbbb03",
                    "border": 1
                }}
            )
        if for_mail:
            # NOTE: xlsxwriter has no autofit column, so resize column width one by one.
            # see https://xlsxwriter.readthedocs.io/worksheet.html
            worksheet = df_wrapper.get_worksheet()
            worksheet.set_column(0,1,25)  # 아이템 명, 브랜드 명
            worksheet.set_column(2,2,50)  # 매니저페이지 주소
        
        return df_wrapper.read_data()

    def __send_mail(self, date, brand_name: str, brand_mails: list, product_data):
        if settings.SERVICE_MODE in [
            "test",
            "debug",
            "local"
        ]:
            return
        
        html = render_to_string("stock_nudging_email.html")
        
        mail = EmailMessage(
            subject="[브랜더진] 인기재고 추가 입고요청",
            body=html,
            from_email="Brandazine <hello@brandazine.com>",
            to=brand_mails,
        )
        mail.content_subtype = "html"
        mail.attach(f"{date}_{brand_name}_인기재고.xlsx", product_data, "application/vnd.ms-excel")
        mail.send()

    def handle(self, *args, **options):
        client = self.__get_athena_client()
        query_parameters = self.__prepare_query_paramters()
        
        try:
            result = execute_athena_query(
                client=client,
                query=HIGH_DEMAND_STOCK,
                database=self.DATABASE,
                output_location="s3://"+self.BUCKET_NAME+"/"+self.ROOT_FOLDER_NAME,
                params=query_parameters,
                max_execution=100,
                execution_interval_seconds=1,
            ) 
        except Exception as error:
            post_message(
                message=f"FAIL TO EXCUTE QUERY, See error\n{error}",
                channel="bot-brand-stock-nudge",
            )
            return

        s3_path = result['QueryExecution']['ResultConfiguration']['OutputLocation']
        filename = re.findall('.*\/(.*)', s3_path)[0]
        file_location = self.ROOT_FOLDER_NAME+"/"+filename
        
        data = self.__get_s3_data(self.BUCKET_NAME, file_location)
        df = self.__read_csv_data(data)
        
        high_demand_stock_data = HighDemandStockFilter(dataframe=df)
        high_demand_stock_data.filter_high_demand_stock()
        df = high_demand_stock_data.get_filtered_items()
        
        data = self.__export_dataframe(df)
        today = start_of_today().strftime("%y%m%d")
        
        post_files(
            content=data,
            filename=f"High-Demand-Stock-{today}.xlsx",
            initial_comment=f"🤖인기재고 목록",
            channels="bot-brand-stock-nudge",
            filetype="xlsx",
            title=f"[{today}] 인기 재고 목록"
        )

        nudge_data = HighDemandStockNudge(df)
        mail_list = nudge_data.get_high_demand_product()

        send_brand_list = Brand.objects.prefetch_related("admins").filter(
            id__in=mail_list.keys(),
            subscription_type__in=[SubscriptionType.NORMAL, SubscriptionType.FREE],
        ).distinct()
        success_emailed_brand, failed_emailed_brand = list(), list()
        for brand in tqdm(send_brand_list):
            dataframe = mail_list[brand.id].filter(items=[
                "상품명",
                "브랜드",
                "매니저 페이지 링크",
                "적정 재고수",
                "입고중 재고",
                "이용중 재고",
                "누적 좋아요 수",
                "1주일간 조회수",
            ])
            try:
                data = self.__export_dataframe(dataframe, True)
                brand_emails = list(brand.admins.values_list("email", flat=True))
                if brand.business_email:
                    brand_emails.append(brand.business_email)
                brand_emails.append("thkim@brandazine.com")
                self.__send_mail(today, brand.name_ko, brand_emails, data)
                nudge_data.save_instock_request(df=mail_list[brand.id])
                success_emailed_brand.append(f"{brand.name_ko}")
            except Exception as error:
                logger.error(f"Fail to mail[{brand.name_ko}], see error\n{error}")
                failed_emailed_brand.append(brand.name_ko)
                continue

        post_message(
            message=(
                f"STOCK REQUEST LOG | TOTAL: [{len(send_brand_list)}]\n"
                "===========================\n"
                f"SUCCESS: {len(success_emailed_brand)}\n"
                "---------------------------\n" + "\n".join(success_emailed_brand) + "\n\n"
                f"FAILED: {len(failed_emailed_brand)}\n"
                "---------------------------\n" + "\n".join(failed_emailed_brand) + "\n\n"
            ),
            channel="bot-brand-stock-nudge",
        )
