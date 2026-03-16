from django.db import models

class Regulation(models.Model):
    code = models.CharField(max_length=10)  # e.g., GR22
    passout_year = models.IntegerField()
    # batch_year = models.IntegerField()

    def __str__(self):
        return f"{self.code} - {self.passout_year}"

class Branch(models.Model):
    code = models.CharField(max_length=10)  # e.g., CSE
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.code

class Section(models.Model):
    name = models.CharField(max_length=5)  # e.g., A, B, C

    def __str__(self):
        return self.name

class Subject(models.Model):
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name

class SubjectOffering(models.Model):
    subject = models.ForeignKey(Subject, on_delete=models.CASCADE)
    regulation = models.ForeignKey(Regulation, on_delete=models.CASCADE)
    branch = models.ForeignKey(Branch, on_delete=models.CASCADE)
    semester = models.CharField(max_length=10)  # e.g., III

class ExamType(models.Model):
    name = models.CharField(max_length=50)  # e.g., Mid1, External

    def __str__(self):
        return self.name

class Student(models.Model):
    roll_number = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=100)
    year = models.CharField(max_length=10)  # I, II, III, IV
    semester = models.CharField(max_length=10)
    phone_number = models.CharField(max_length=15, blank=True, null=True)
    regulation = models.ForeignKey(Regulation, on_delete=models.CASCADE)
    branch = models.ForeignKey(Branch, on_delete=models.CASCADE)
    section = models.ForeignKey(Section, on_delete=models.CASCADE)

    def __str__(self):
        return self.roll_number


class Marks(models.Model):
    student = models.ForeignKey(Student, on_delete=models.CASCADE)
    subject = models.ForeignKey(Subject, on_delete=models.CASCADE)
    exam_type = models.ForeignKey(ExamType, on_delete=models.CASCADE)
    marks_obtained = models.IntegerField()
    max_marks = models.IntegerField(default=100)
