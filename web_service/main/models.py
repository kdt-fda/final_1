from django.db import models


class Basic(models.Model):
    stock_code = models.CharField(primary_key=True, max_length=10) # 주식코드
    corp_code = models.CharField(max_length=15, blank=True, null=True) # dart 기업 코드
    corp_name = models.CharField(max_length=100, blank=True, null=True) # 기업명
    est_dt = models.CharField(max_length=20, blank=True, null=True) # 설립일
    ipo = models.CharField(max_length=20, blank=True, null=True)  #상장일
    ind_code = models.ForeignKey('IndBasic', models.DO_NOTHING, db_column='ind_code', blank=True, null=True) # 산업 코드(업종 코드)
    is_active = models.IntegerField(blank=True, null=True) # 활성화 여부, True만 홈페이지에 보여줌
    updated_at = models.DateTimeField() # db 업데이트 된 날짜 확인용

    class Meta:
        managed = False # Django가 테이블을 생성/삭제하지 않음
        db_table = 'BASIC' # 실제 DB 테이블 이름


class BokIo(models.Model):
    ind_code = models.ForeignKey('IndBasic', models.DO_NOTHING, db_column='ind_code')
    io_code = models.CharField(max_length=10)
    io_name = models.CharField(max_length=100)

    class Meta:
        managed = False
        db_table = 'BOK_IO'
        unique_together = (('ind_code', 'io_code'),)


class CompanyFinance(models.Model):
    stock_code = models.CharField(max_length=10)
    corp_code = models.CharField(max_length=15)
    biz_year = models.IntegerField()
    currency = models.CharField(max_length=20, blank=True, null=True)
    total_assets = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    cash_and_equivalents = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    current_assets = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    accounts_receivable = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    liabilities = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    current_liabilities = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    equity = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    capital_stock = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    revenue_latest = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    revenue_1y_ago = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    revenue_2y_ago = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    gross_profit = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    net_income = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    cashholding_ratio_pct = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    sales_growth_rate_pct = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    gross_margin_pct = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    roe = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    net_margin_pct = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    debt_ratio_pct = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    current_ratio_pct = models.DecimalField(max_digits=20, decimal_places=6, blank=True, null=True)
    maj_shareholders = models.TextField(blank=True, null=True)
    source_report_num = models.CharField(max_length=20, blank=True, null=True)
    source_report_nm = models.CharField(max_length=255, blank=True, null=True)
    source_report_date = models.DateField(blank=True, null=True)
    match_status = models.CharField(max_length=50, blank=True, null=True)
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'COMPANY_FINANCE'
        unique_together = (('corp_code', 'biz_year'),)


class CompanyStock(models.Model):
    stock_code = models.ForeignKey(Basic, models.DO_NOTHING, db_column='stock_code')
    reference_date = models.DateField()
    open_price = models.IntegerField(blank=True, null=True)
    prev_close_price = models.IntegerField(blank=True, null=True)
    close_price = models.IntegerField(blank=True, null=True)
    price_change = models.IntegerField(blank=True, null=True)
    high_price = models.IntegerField(blank=True, null=True)
    low_price = models.IntegerField(blank=True, null=True)
    wk52_high = models.IntegerField(blank=True, null=True)
    wk52_low = models.IntegerField(blank=True, null=True)
    book_value = models.IntegerField(blank=True, null=True)
    mktcap = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    shares_btj = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    trdvol = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    acc_trdvol = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    acc_trdval = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    bas_trdval = models.DecimalField(max_digits=20, decimal_places=0, blank=True, null=True)
    foreign_ratio = models.DecimalField(max_digits=9, decimal_places=4, blank=True, null=True)
    fluc_rt = models.DecimalField(max_digits=9, decimal_places=4, blank=True, null=True)
    dps = models.DecimalField(max_digits=18, decimal_places=4, blank=True, null=True)
    eps = models.DecimalField(max_digits=18, decimal_places=4, blank=True, null=True)
    eps_1yearago = models.DecimalField(max_digits=18, decimal_places=4, blank=True, null=True)
    dividend_yield = models.DecimalField(max_digits=6, decimal_places=3, blank=True, null=True)
    per = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)
    pbr = models.DecimalField(max_digits=10, decimal_places=4, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'COMPANY_STOCK'
        unique_together = (('stock_code', 'reference_date'),)


class CorpCodeMap(models.Model):
    stock_code = models.CharField(primary_key=True, max_length=10)
    corp_code = models.CharField(max_length=15)
    updated_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'CORP_CODE_MAP'


class DartFin(models.Model):
    corp_code = models.CharField(max_length=15)
    biz_year = models.IntegerField()
    reprt_code = models.CharField(max_length=10)
    revenue = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    op_profit = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    net_income = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    equity = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    disclosure_date = models.DateField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'DART_FIN'
        unique_together = (('corp_code', 'biz_year', 'reprt_code'),)


class FeatureBasic(models.Model):
    pk = models.CompositePrimaryKey('stock_code', 'date')
    stock_code = models.CharField(max_length=10, db_comment='stock code')
    date = models.DateField(db_comment='trade date')
    corp_name = models.CharField(max_length=100, blank=True, null=True, db_comment='company name')
    corp_code = models.CharField(max_length=8, blank=True, null=True, db_comment='DART corp code')
    is_listed_on_date = models.IntegerField(blank=True, null=True, db_comment='1 listed / 0 not listed / NULL unknown')
    is_active_now = models.IntegerField(blank=True, null=True, db_comment='1 active now / 0 delisted / NULL unknown')

    class Meta:
        managed = False
        db_table = 'FEATURE_BASIC'
        db_table_comment = 'Base feature table for stock modeling'


class FeatureRaw(models.Model):
    # stock_code랑 date 복합 외래키는 장고에서 지원 안한다고 해서 일단 일반 필드로 교체
    stock_code = models.CharField(max_length=10, db_column='stock_code')
    date = models.DateField(db_column='date')
    
    close = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    trading_value = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    foreign_netbuy_value = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    inst_netbuy_value = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    per = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    pbr = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    mkt_cap = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'FEATURE_RAW'
        unique_together = (('stock_code', 'date'),)


class FeatureRawD(models.Model):
    stock_code = models.CharField(max_length=10)
    corp_code = models.CharField(max_length=15)
    net_income = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    equity = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    op_profit = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    revenue = models.DecimalField(max_digits=24, decimal_places=6, blank=True, null=True)
    disclosure_date = models.DateField(blank=True, null=True)
    biz_year = models.IntegerField()
    created_at = models.DateTimeField()
    updated_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'FEATURE_RAW_D'
        unique_together = (('corp_code', 'biz_year'),)


class IndBasic(models.Model):
    ind_code = models.CharField(primary_key=True, max_length=10)
    ind_name = models.CharField(max_length=60, blank=True, null=True)
    ind_def = models.TextField(blank=True, null=True)
    bok_code = models.CharField(max_length=10, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'IND_BASIC'


class IndBok(models.Model):
    pk = models.CompositePrimaryKey('bok_code', 'year')
    bok_code = models.CharField(max_length=10)
    year = models.IntegerField()
    asset_growth_rate = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    sales_growth_rate = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'IND_BOK'


class IndIo(models.Model):
    link_id = models.AutoField(primary_key=True)
    trade_vol = models.DecimalField(max_digits=18, decimal_places=2)
    year = models.IntegerField()
    out_io_code = models.CharField(max_length=10)
    in_io_code = models.CharField(max_length=10)

    class Meta:
        managed = False
        db_table = 'IND_IO'
        unique_together = (('year', 'out_io_code', 'in_io_code'),)


class Label(models.Model):
    stock_code = models.CharField(max_length=10)
    asof_date = models.DateField()
    alpha = models.DecimalField(max_digits=18, decimal_places=6, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'LABEL'
        unique_together = (('stock_code', 'asof_date'),)


class MarketIndex(models.Model):
    date = models.DateField(unique=True)
    kospi = models.DecimalField(db_column='KOSPI', max_digits=18, decimal_places=6, blank=True, null=True)  # Field name made lowercase.
    kosdaq = models.DecimalField(db_column='KOSDAQ', max_digits=18, decimal_places=6, blank=True, null=True)  # Field name made lowercase.

    class Meta:
        managed = False
        db_table = 'MARKET_INDEX'


class Report(models.Model):
    stock_code = models.ForeignKey(Basic, models.DO_NOTHING, db_column='stock_code', blank=True, null=True)
    report_num = models.CharField(unique=True, max_length=50, blank=True, null=True)
    report_name = models.CharField(max_length=255, blank=True, null=True)
    report_date = models.CharField(max_length=20, blank=True, null=True)
    history_origin = models.TextField(blank=True, null=True)
    outline_origin = models.TextField(blank=True, null=True)
    product_origin = models.TextField(blank=True, null=True)
    sales_origin = models.TextField(blank=True, null=True)
    history_ai = models.TextField(blank=True, null=True)
    outline_ai = models.TextField(blank=True, null=True)
    product_ai = models.TextField(blank=True, null=True)
    product_ratio_ai = models.TextField(blank=True, null=True)
    sales_ai = models.TextField(blank=True, null=True)
    updated_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'REPORT'


class AuthGroup(models.Model):
    name = models.CharField(unique=True, max_length=150)

    class Meta:
        managed = False
        db_table = 'auth_group'


class AuthGroupPermissions(models.Model):
    id = models.BigAutoField(primary_key=True)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)
    permission = models.ForeignKey('AuthPermission', models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_group_permissions'
        unique_together = (('group', 'permission'),)


class AuthPermission(models.Model):
    name = models.CharField(max_length=255)
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING)
    codename = models.CharField(max_length=100)

    class Meta:
        managed = False
        db_table = 'auth_permission'
        unique_together = (('content_type', 'codename'),)


class AuthUser(models.Model):
    password = models.CharField(max_length=128)
    last_login = models.DateTimeField(blank=True, null=True)
    is_superuser = models.IntegerField()
    username = models.CharField(unique=True, max_length=150)
    first_name = models.CharField(max_length=150)
    last_name = models.CharField(max_length=150)
    email = models.CharField(max_length=254)
    is_staff = models.IntegerField()
    is_active = models.IntegerField()
    date_joined = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'auth_user'


class AuthUserGroups(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    group = models.ForeignKey(AuthGroup, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_user_groups'
        unique_together = (('user', 'group'),)


class AuthUserUserPermissions(models.Model):
    id = models.BigAutoField(primary_key=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)
    permission = models.ForeignKey(AuthPermission, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'auth_user_user_permissions'
        unique_together = (('user', 'permission'),)


class DjangoAdminLog(models.Model):
    action_time = models.DateTimeField()
    object_id = models.TextField(blank=True, null=True)
    object_repr = models.CharField(max_length=200)
    action_flag = models.PositiveSmallIntegerField()
    change_message = models.TextField()
    content_type = models.ForeignKey('DjangoContentType', models.DO_NOTHING, blank=True, null=True)
    user = models.ForeignKey(AuthUser, models.DO_NOTHING)

    class Meta:
        managed = False
        db_table = 'django_admin_log'


class DjangoContentType(models.Model):
    app_label = models.CharField(max_length=100)
    model = models.CharField(max_length=100)

    class Meta:
        managed = False
        db_table = 'django_content_type'
        unique_together = (('app_label', 'model'),)


class DjangoMigrations(models.Model):
    id = models.BigAutoField(primary_key=True)
    app = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    applied = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_migrations'


class DjangoSession(models.Model):
    session_key = models.CharField(primary_key=True, max_length=40)
    session_data = models.TextField()
    expire_date = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'django_session'


class Users(models.Model):
    name = models.CharField(max_length=50)
    email = models.CharField(max_length=100, blank=True, null=True)
    created_at = models.DateTimeField()

    class Meta:
        managed = False
        db_table = 'users'
