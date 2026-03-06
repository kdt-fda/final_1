from django.db import models

class Basic(models.Model):
    stock_code = models.CharField(max_length=10, primary_key=True)
    corp_name = models.CharField(max_length=100)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = 'basic' # 실제 DB 테이블 이름
        managed = False    # Django가 테이블을 생성/삭제하지 않음

class Report(models.Model):
    # Basic 테이블의 stock_code와 연결
    stock_code = models.ForeignKey(Basic, on_delete=models.CASCADE, db_column='stock_code')
    report_num = models.CharField(max_length=50, unique=True)
    report_date = models.CharField(max_length=20)
    # AI 분석 결과들
    history_ai = models.TextField(null=True)
    outline_ai = models.TextField(null=True)
    product_ai = models.TextField(null=True)
    product_ratio_ai = models.TextField(null=True)
    sales_ai = models.TextField(null=True)

    class Meta:
        db_table = 'report' # 실제 DB 테이블 이름
        managed = False # Django가 테이블을 생성/삭제하지 않음