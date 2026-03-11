from django.shortcuts import render, get_object_or_404, redirect
from .models import Basic, Report
import json

def home(request):
    count = Report.objects.count()
    return render(request, 'home.html', {'count': count})

def search(request):
    query = request.GET.get('q', '')
    
    if query:
        # 기업명에 검색어가 포함된 데이터를 찾음. 여기서 활성화 된 애만 검색 가능함
        # 여기서 원하는 속성만 가져오려면 뒤에 .values('corp_name', 'stock_code') 처럼 쓰면 됨
        results = Basic.objects.filter(corp_name__icontains=query, is_active=True) # corp_name에서 대소문자 구분 없이, query가 포함되고, is_active=True인 데이터 가져옴
        
        # 중복이 없으므로, 결과가 딱 1개라면 바로 AI 페이지로 이동
        if results.count() == 1:
            return redirect('ai_page', stock_code=results.first().stock_code)
    else:
        results = []

    # 결과가 없거나 2개 이상(부분 일치 등)일 때만 검색 결과 리스트를 보여줌
    return render(request, 'search.html', {'results': results, 'query': query})

def ai_page(request, stock_code):
    company = get_object_or_404(Basic, stock_code=stock_code) # BASIC 테이블에 있는 stock_code만 가져오게 함
    report = Report.objects.filter(stock_code=stock_code).first() # 해당 종목의 report 테이블을 가져옴

    # 데이터 유효성 검사 (None, 'None', '[]' 등 체크)
    def is_invalid(value):
        return value in [None, 'None', '[]', 'NULL', [], 'null', '', 'NULL']

    # 주요 연혁 및 제품 비중 노출 여부 판단
    show_history = report and not is_invalid(report.history_ai) # report가 있으면서 history_ai랑 product_ratio_ai는 None이 아니어야 홈페이지에 띄우기 위한 용도
    show_ratio = report and not is_invalid(report.product_ratio_ai)

    ratio_data = []
    if show_ratio:
        try:
            # JSON 문자열 파싱, 딕셔너리 형태로 전환해서 넘김
            ratio_data = json.loads(report.product_ratio_ai)
        except (json.JSONDecodeError, TypeError):
            show_ratio = False # json 못가져오면 show_ratio False로 해서 안보여줌

    context = {
        'company': company,
        'report': report,
        'show_history': show_history,
        'show_ratio': show_ratio,
        'ratio_data': ratio_data, # 템플릿의 json_script에서 사용
    }
    return render(request, 'ai_page.html', context)

def industry(request, stock_code): # 혜원님 코드로 수정
    company = get_object_or_404(Basic, stock_code=stock_code)
    
    context = {
        'company': company,
    }
    return render(request, 'industry.html', context)
