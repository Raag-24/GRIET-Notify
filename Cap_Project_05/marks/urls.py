"""from django.urls import path
from . import views

urlpatterns = [
    path('', views.fetch_marks_table, name='fetch_marks'),
    # path('send_sms/', views.send_sms_to_all, name='send_sms_to_all'),
    path('send_sms/', views.send_sms_view, name='send_sms'),
]
"""
from django.urls import path
from . import views

urlpatterns = [
    path('', views.fetch_marks_table, name='fetch_marks'),
    path('send_sms/', views.send_sms_view, name='send_sms'),
]