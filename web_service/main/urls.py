from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('search/', views.search, name='search'),
    path('ai_page/<str:stock_code>/', views.ai_page, name='ai_page'),
    path('industry/<str:stock_code>/', views.industry, name='industry'),
]