# Hospital_Appointments_Manager

## Project Description:
Distributed relational database system designed for a hospital to manage its appointments' data. 
Data is stored in one of two databases based on the hash value of the associated DepartmentID.
Worked on in collaboration with Omar Alkhadra who made a web based application for the database logic and command line
code I did for this project. The command line application logic implemented here is designed for database
managers and gives full access to the data. On the other hand, the web application is designed for
receptionists only and has certain limitations on access for data integrity reasons.

## Requirements for running this project:
- To install the required packages, navigate to the directory where you have
  installed this project: cd path_to_folder/Hospital_Appointments_Manager
  - Run the following from the command line:
    - pip install -r requirements.txt
- To run this project, you need to have MySQL installed locally and have a MySQL username and password.
  - If you need to install MySQL, you can visit the following link and follow installation instructions:
    https://dev.mysql.com/doc/mysql-installation-excerpt/5.7/en/
- Prior to running, log in to MySQL and run the following code in MySQL to create the databases: 
  - CREATE DATABASE database1;
  - CREATE DATABASE database2;
- At the top of the code in hospital_db.py, there is an engine url dictionary in lines 29-30. 
  After creating the databases within MySQl, please update the url string for each database 
  with your password and username in the following format:
  - 'mysql+mysqlconnector://USERNAME:PASSWORD@localhost/database1'
  - Fill in the username and password as indicated above for each database url.
  - Alternatively, uncomment the login call at the top of main to login via the command line each time
    you run the script.

## Instructions on how to call each function from the command line:
- In the command line, navigate to the folder where you have downloaded this directory: cd path/to/folder
 Note: The following examples use backslashes in the JSON objects as formatted for Windows. Reformat as needed for Mac.
  - For some reason, the backslashes don't show on GitHub prior to doing git pull: perform git pull or reformat as needed.


## Adding data:
- General format: python hotpital_db.py add_operation json_object_with_data_to add
- Departments example: python hospital_db.py add_department "{\"DepartmentID\": 1, \"DepartmentName\": \"Cardiology\", \"TotalRooms\": 100}"
- Receptionists example: python hospital_db.py add_receptionist "{\"EmployeeID\": 211111, \"LastName\": \"Smith\", \"FirstName\": \"John\", \"DepartmentID\": 1}"
  - Note: EmployeeID must be 6 digits
- Practitioners example: python hospital_db.py add_practitioner "{\"EmployeeID\": 111111, \"LastName\": \"Jones\", \"FirstName\": \"Amanda\", \"LicenseNumber\": 4567,  \"Title\": \"Doctor\", \"DepartmentID\": 1, \"Specialty\": \"General heart disease\"}"
  - Note: EmployeeID must be 6 digits
- Patients example: python hospital_db.py add_patient "{\"PatientID\": 1000, \"LastName\": \"Brown\", \"FirstName\": \"Ollie\", \"DOB\": \"1991-10-10\",  \"Gender\": \"Female\",  \"Insurance\": \"Aetna\", \"PastProcedures\":\"MR Scan\", \"Notes\":  \"Lung/breathing problems\",  \"DepartmentID\": 1}"
  - Note: PatientID must be 4 digits
- Appointments example: python hospital_db.py add_appointment "{\"ReceptionistID\": 211111, \"PatientID\": 1000, \"PractitionerID\": 111111, \"DepartmentID\": 1, \"AppointmentDate\": \"2024-03-19\", \"AppointmentTime\": \"10:00\", \"Notes\": \"Follow-up appointment\"}

## Modifying data:
- General format:  python hospital_db.py modify_operation json_object_with_filter_requirements_for_rows_to_update 
  json_object_with_new_values_to_insert
  - Exception for departments: python hospital_db.py modify_department dept_id json_object_with_new_values
- Departments example: python hospital_db.py modify_department 1 "{\"DepartmentName\": \"Emergency\"}"
- Receptionists example: python hospital_db.py modify_receptionist "{\"EmployeeID\": 211111}" "{\"LastName\": \"Miller\"}"
- Practitioners example: python hospital_db.py modify_practitioner "{\"Title\": \"Doctor\"}" "{\"Title\": \"Medical Doctor\"}"  
- Patients example: python hospital_db.py modify_patient "{\"PatientID\": 1000}"  "{\"Insurance\": \"Kasier\"}" 
- Appointments example: python hospital_db.py modify_appointment "{\"AppointmentTime\": \"10:00\"}" "{\"AppointmentTime\": \"10:30\"}"

## Deleting data:
- Note: if you are testing along with these examples, please test delete after retrieve to have data to retrieve.
- Note, the order in which you delete it is important due to foreign keys defined with cascade on update and delete with this small of a dataset. 
  Departments should be deleted last.
- General format: python hospital_db.py delete_operation json_object_with_filter_requirements_for_rows_to_delete
  - Exception for departments: python hospital_db.py delete_department dept_id
- Appointments example: python hospital_db.py delete_appointment  "{\"AppointmentTime\": \"10:30\"}"
- Patient example: python hospital_db.py delete_patient "{\"PatientID\": 1000}"
- Receptionists example: python hospital_db.py delete_receptionist  "{\"EmployeeID\": 211111}"
- Practitioner example: python hospital_db.py delete_practitioner "{\"LastName\": \"Jones\", \"FirstName\": \"Amanda\", \"LicenseNumber\": 4567}"
- Departments example: python hospital_db.py delete_department 1

## Retrieving data:
- General format: python hospital_db.py get_operation json_object_with_filter_requirements_for_rows_to_retrieve
  - Note: the json_object is an optional argument, if not provided it defaults to retrieving all rows for that table
    across both databases.
  - Exceptions: get_practitioner_for, get_patient_of
    - For these, the format is: python hospital_db.py get_operation id_var optional_list_of_attribute_names_to_retrieve
    - Note: the id_var is the PractitionerID in get_patient_of that you want to see patients of and
      PatientID for get_practitioner_for that you want to see practitioners for.
- Departments example: python hospital_db.py get_department "{\"DepartmentID\": 1}"
- Receptionists example: python hospital_db.py get_receptionist "{\"DepartmentID\": 1}"
- Practitioners example: python hospital_db.py get_practitioner "{\"Title\": \"Medical Doctor\"}"
- Patients example: python hospital_db.py get_patient "{ \"LastName\":  \"Brown\"}"
- Appointments example: python hospital_db.py get_appointment "{ \"AppointmentDate\": \"2024-03-19\"}"

- Examples to retrieve all for departments, patients, practitioners, receptionists, and appointment tables:
  - python hospital_db.py get_department
  - python hospital_db.py get_receptionist
  - python hospital_db.py get_practitioner
  - python hospital_db.py get_patient
  - python hospital_db.py get_appointment
  
- Patient_Of examples:
  - get_patient_of: python hospital_db.py get_patients_of 111111
    - With optional list of attributes to return: python hospital_db.py get_patients_of 111111 "[FirstName, LastName]"
  - get_practitioners_for: python hospital_db.py get_practitioners_for 1000
    - With optional list of attributes to return: python hospital_db.py get_practitioners_for 1000 "[FirstName, LastName]"

