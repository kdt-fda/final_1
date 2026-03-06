from django.shortcuts import render, get_object_or_404, redirect
from .models import Basic, Report

def home(request):
    count = Report.objects.count()
    return render(request, 'home.html', {'count': count})

def search(request):
    query = request.GET.get('q', '')
    
    if query:
        # 기업명에 검색어가 포함된 데이터를 찾습니다.
        results = Basic.objects.filter(corp_name__icontains=query, is_active=True)
        
        # 중복이 없으므로, 결과가 딱 1개라면 바로 AI 페이지로 이동시킵니다.
        if results.count() == 1:
            return redirect('ai_page', stock_code=results.first().stock_code)
    else:
        results = []

    # 결과가 없거나 2개 이상(부분 일치 등)일 때만 검색 결과 리스트를 보여줍니다.
    return render(request, 'search.html', {'results': results, 'query': query})

def ai_page(request, stock_code):
    # 종목코드로 기본 정보와 리포트를 가져옵니다.
    company = get_object_or_404(Basic, stock_code=stock_code)
    report = Report.objects.filter(stock_code=stock_code).first()
    
    context = {
        'company': company,
        'report': report
    }
    return render(request, 'ai_page.html', context)




# 여기서 함수 이름이랑 .html 이름 나중에 수정할 것, urls.py에서 연결할 주소 명칭도 적절하게 바꿔야됨
def a(request):
    return render(request, 'a.html')

def about(request):
    return render(request, 'about.html')

def stats(request):
    return render(request, 'stats.html')