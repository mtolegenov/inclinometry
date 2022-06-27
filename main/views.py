import django.core.exceptions
import pandas as pd
from django.http import HttpResponse
from django.shortcuts import render
import psycopg2
from main.script.utils import main
import sqlalchemy

# Create your views here.


def get_data_from_file_in_memory(file_content):
    data = pd.ExcelFile(file_content)
    return data


def excel_file(request):
    if request.method == "POST":
        excel_f = get_data_from_file_in_memory(request.FILES['document'])
        excel_byte = request.FILES['document']
        mail = request.POST.get('mail')
        print("SECOND COMMIT")
        return HttpResponse(main(mail, excel_f, excel_byte))
    return render(request, 'main/index.html')
