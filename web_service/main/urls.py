from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('search/', views.search, name='search'),
    path('overview/', views.overview, name='overview'),
    path('ai_page/', views.ai_page_preview, name='ai_page_preview'),
    path('ai_page/<str:stock_code>/', views.ai_page, name='ai_page'),
    path('finance/', views.finance, name='finance'),
    path('industry/<str:stock_code>/', views.industry, name='industry'),
    path('about/', views.about, name='about'),
    path('stats/', views.stats, name='stats'),
]

