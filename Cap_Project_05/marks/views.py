"""from django.shortcuts import render
from .models import *
from collections import defaultdict

from twilio.rest import Client
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
import json
from django.conf import settings

def fetch_marks_table(request):
    if request.method == 'POST':
        regulation_id = request.POST.get('regulation')
        exam_type_id = request.POST.get('exam_type')
        year = request.POST.get('year')
        semester = request.POST.get('semester')
        branch_id = request.POST.get('branch')
        section_id = request.POST.get('section')

        exam_type = ExamType.objects.get(id=exam_type_id)

        students = Student.objects.filter(
            regulation_id=regulation_id,
            branch_id=branch_id,
            section_id=section_id,
            semester=semester
        )

        offerings = SubjectOffering.objects.filter(
            regulation_id=regulation_id,
            branch_id=branch_id,
            semester=semester
        ).select_related('subject')

        subjects = [o.subject for o in offerings]

        marks = Marks.objects.filter(
            student__in=students,
            subject__in=subjects,
            exam_type_id=exam_type_id
        ).select_related('student', 'subject')

        student_marks = defaultdict(lambda: defaultdict(str))

        for mark in marks:
            student_marks[mark.student.roll_number][mark.subject.name] = mark.marks_obtained

        final_rows = []
        for student in students:
            row = {
                'student_id': student.roll_number,
                'student_name': student.name,
                'phone_number': student.phone_number
            }
            for subject in subjects:
                row[subject.name] = student_marks[student.roll_number].get(subject.name, '-')
            final_rows.append(row)
        
        print("Students:", students.count())
        print("Offerings:", offerings.count())
        print("Subjects:", subjects)
        print("Marks:", marks.count())
        print(f"Rows: {final_rows}")
        print(f"Subjects: {[s.name for s in subjects]}")

        return render(request, 'marks/marks_table.html', {
            'rows': final_rows,
            'subjects': [s.name for s in subjects],
            'exam_type_name': exam_type.name
        })

    context = {
        'regulations': Regulation.objects.all(),
        'exam_types': ExamType.objects.all(),
        'branches': Branch.objects.all(),
        'sections': Section.objects.all(),
    }
    return render(request, 'marks/marks_form.html', context)



@csrf_exempt
def send_sms_view(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        student_ids = data.get('selected_students', [])

        # Twilio credentials from settings.py
        account_sid = settings.TWILIO_ACCOUNT_SID
        auth_token = settings.TWILIO_AUTH_TOKEN
        from_number = settings.TWILIO_PHONE_NUMBER

        client = Client(account_sid, auth_token)
        result = []

        for student_id in student_ids:
            try:
                student = StudentsData.objects.get(id=student_id)
                msg = f"Hi {student.name}, your marks are: Sub1: {student.sub1_marks}, Sub2: {student.sub2_marks}, Sub3: {student.sub3_marks}"
                
                phone = student.phone_number.strip()
                if not phone.startswith('+91'):
                    phone = '+91' + phone
                
                message = client.messages.create(
                    body=msg,
                    from_=from_number,
                    to=phone
                )
                print(f"Message SID: {message.sid}")
                print(f"To: {message.to}")
                print(f"Status: {message.status}")
                print(f"Error Code: {message.error_code}")
                print(f"Error Message: {message.error_message}")
                result.append({"id": student_id, "status": "success"})
            except Exception as e:
                result.append({"id": student_id, "status": "fail", "error": str(e)})
        

        return JsonResponse({"results": result})
    print(f"Message SID: {message.sid}")
    print(f"To: {message.to}")
    print(f"Status: {message.status}")
    print(f"Error Code: {message.error_code}")
    print(f"Error Message: {message.error_message}")
    
"""
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from .models import *
from collections import defaultdict
import json
import os
from twilio.rest import Client
from django.conf import settings

def fetch_marks_table(request):
    if request.method == 'POST':
        regulation_id = request.POST.get('regulation')
        exam_type_id = request.POST.get('exam_type')
        year = request.POST.get('year')
        semester = request.POST.get('semester')
        branch_id = request.POST.get('branch')
        section_id = request.POST.get('section')

        exam_type = ExamType.objects.get(id=exam_type_id)

        students = Student.objects.filter(
            regulation_id=regulation_id,
            branch_id=branch_id,
            section_id=section_id,
            semester=semester
        )

        offerings = SubjectOffering.objects.filter(
            regulation_id=regulation_id,
            branch_id=branch_id,
            semester=semester
        ).select_related('subject')

        subjects = [o.subject for o in offerings]

        marks = Marks.objects.filter(
            student__in=students,
            subject__in=subjects,
            exam_type_id=exam_type_id
        ).select_related('student', 'subject')

        student_marks = defaultdict(lambda: defaultdict(str))

        for mark in marks:
            student_marks[mark.student.roll_number][mark.subject.name] = mark.marks_obtained

        final_rows = []
        for student in students:
            row = {
                'student_id': student.roll_number,
                'student_name': student.name,
                'phone_number': student.phone_number
            }
            for subject in subjects:
                row[subject.name] = student_marks[student.roll_number].get(subject.name, '-')
            final_rows.append(row)
        
        print("Students:", students.count())
        print("Offerings:", offerings.count())
        print("Subjects:", subjects)
        print("Marks:", marks.count())
        print(f"Rows: {final_rows}")
        print(f"Subjects: {[s.name for s in subjects]}")

        return render(request, 'marks/marks_table.html', {
            'rows': final_rows,
            'subjects': [s.name for s in subjects],
            'exam_type_name': exam_type.name
        })

    context = {
        'regulations': Regulation.objects.all(),
        'exam_types': ExamType.objects.all(),
        'branches': Branch.objects.all(),
        'sections': Section.objects.all(),
    }
    return render(request, 'marks/marks_form.html', context)


@csrf_exempt
@require_http_methods(["POST"])
def send_sms_view(request):
    try:
        # Parse the JSON data from request
        data = json.loads(request.body)
        selected_students = data.get('students', [])
        exam_type_name = data.get('exam_type', '')
        
        if not selected_students:
            return JsonResponse({'error': 'No students selected'}, status=400)
        
        # Initialize Twilio client
        try:
            client = Client(settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN)
            twilio_phone = settings.TWILIO_PHONE_NUMBER
        except Exception as e:
            return JsonResponse({'error': f'Twilio configuration error: {str(e)}'}, status=500)
        
        results = []
        
        for student_data in selected_students:
            try:
                student_id = student_data.get('student_id')
                student_name = student_data.get('student_name')
                phone_number = student_data.get('phone_number')
                marks = student_data.get('marks', {})
                
                # Skip if no phone number
                if not phone_number:
                    results.append({
                        'student_id': student_id,
                        'student_name': student_name,
                        'status': 'failed',
                        'message': 'No phone number available'
                    })
                    continue
                
                # Format phone number for India
                if not phone_number.startswith('+91'):
                    if phone_number.startswith('91'):
                        phone_number = '+' + phone_number
                    else:
                        phone_number = '+91' + phone_number.lstrip('+')
                
                # Create subject-wise marks string
                marks_text = []
                for subject, score in marks.items():
                    if score != '-' and score != '':
                        marks_text.append(f"{subject}: {score}")
                
                if not marks_text:
                    results.append({
                        'student_id': student_id,
                        'student_name': student_name,
                        'status': 'failed',
                        'message': 'No marks available'
                    })
                    continue
                
                # Create SMS message
                message_body = f"Hello {student_name}, your results for {exam_type_name} are: {', '.join(marks_text)}"
                
                # Send SMS using Twilio
                message = client.messages.create(
                    body=message_body,
                    from_=twilio_phone,
                    to=phone_number
                )
                
                # Success response
                results.append({
                    'student_id': student_id,
                    'student_name': student_name,
                    'status': 'success',
                    'message': 'SMS sent successfully',
                    'twilio_sid': message.sid
                })
                
            except Exception as e:
                # Individual student SMS failure
                results.append({
                    'student_id': student_data.get('student_id', 'Unknown'),
                    'student_name': student_data.get('student_name', 'Unknown'),
                    'status': 'failed',
                    'message': f'SMS failed: {str(e)}'
                })
        
        return JsonResponse({
            'success': True,
            'results': results,
            'total_processed': len(results)
        })
        
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON data'}, status=400)
    except Exception as e:
        return JsonResponse({'error': f'Server error: {str(e)}'}, status=500)