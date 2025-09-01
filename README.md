
# PID Detection

## Step 1: Create project on API 
https://r23s7xh7ri.execute-api.eu-west-1.amazonaws.com/prod/docs#/Project/route_project_post

## Step 2: Upload PID file on API
https://r23s7xh7ri.execute-api.eu-west-1.amazonaws.com/prod/docs#/Pid_file/upload_pid_file_upload_post

## Step 3 Upload equipment list on API
https://r23s7xh7ri.execute-api.eu-west-1.amazonaws.com/prod/docs#/Equipment/upload_equipment_upload_post
This is optional but can help the tag detection a lot
You can upload an excel file which consists of headers and a values. So make sure to remove all other stuff from the Excel.

## Step 4: Process PID file via API
https://r23s7xh7ri.execute-api.eu-west-1.amazonaws.com/prod/docs#/Pid_file/process_pid_file_process_post


## Step 5: Run plotting of tags 
in the directory 'deployment/assets/lambda/process_pid_pdf/src', is the file plot_results.py

You will have to install some python libraries to make it work..
