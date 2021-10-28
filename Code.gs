/**
 * 
 * CHANGE THESE VARIABLES PER YOUR PREFERENCES
 * 
 */
const PIPELINE_TAB_NAME = 'pipelines';
const LOG_TAB_NAME = 'log';
const DATE_FORMAT = 'yyyy-MM-dd HH:mm:ss';
var EMAIL_SUBJECT = 'BigQuery Pipeline Notification';
var EMAIL_FOOTER = 'Managed from Google Apps Script'

/**
 * 
 * DO NOT CHANGE ANYTHING BELOW THIS COMMENT BLOCK
 * 
 */

/**
* Job to process rows in sheet and load CSVs if found
*/
function job_run_pipelines_from_google_sheet() {
  
  // Log starting of the script
  log_event_to_google_sheet_('function job_run_pipelines_from_google_sheet starting', LOG_TAB_NAME, DATE_FORMAT);
  
  // get current spreadsheet
  var ss = SpreadsheetApp.getActiveSpreadsheet();

  // get TimeZone
  var timeZone = ss.getSpreadsheetTimeZone();
  
  // get Data sheet
  var dataSheet = ss.getSheetByName(PIPELINE_TAB_NAME);
  
  // get all data as a 2-D array
  var data = dataSheet.getDataRange().getValues();
  
  // create a name:value pair array to send the data to the next Function
  var spreadsheetData = {ss:ss, timeZone:timeZone, dataSheet:dataSheet, data:data};
  
  // run Function to create Google Folders
  var doLoading = search_and_load_files_(spreadsheetData);
  
  // check success status
  if (doLoading) {
    log_event_to_google_sheet_('function job_run_pipelines_from_google_sheet finished successfully', LOG_TAB_NAME, DATE_FORMAT);
  }
  else {
    // script completed with error
    // display Toast notification
    log_event_to_google_sheet_('function job_run_pipelines_from_google_sheet finished with errors. Check additional logs.', LOG_TAB_NAME, DATE_FORMAT);
  }
  
}

/**
* Loop through each row and create folders, set permissions
*/
function search_and_load_files_(spreadsheetData) {
  
  // extract data from name:value pair array
  var ss = spreadsheetData['ss'];
  var timeZone = spreadsheetData['timeZone'];
  var dataSheet = spreadsheetData['dataSheet']; 
  var data = spreadsheetData['data'];

  // get last row number so we know when to end the loop
  var lastRow = dataSheet.getLastRow();

  // start of loop to go through each row iteratively
  for (var i=1; i<lastRow; i++) {
    
    // extract values from row of data for easier reference below
    var pipeline_name = data[i][0];
    var pipeline_description = data[i][1];
    var source_drive_folder_id = data[i][2]
    var processed_drive_folder_id = data[i][3];
    var source_format = data[i][4];
    var write_disposition = data[i][5];
    var skip_leading_rows = data[i][6];
    var bq_project_id = data[i][7];
    var bq_dataset_id = data[i][8];
    var bq_table_id = data[i][9];
    var status = data[i][10];
    var email_recipients = data[i][11];
    
    // only perform this row if the folder ID is blank
    if(status == 'enabled') {

      log_event_to_google_sheet_('Processing Row - Pipeline Enabled: ' + pipeline_name, LOG_TAB_NAME, DATE_FORMAT);

      var files = get_pending_csv_files_(source_drive_folder_id);
      
      // loop through the files and load them
      while (files.hasNext()){
        var file = files.next();

        var this_file_id = file.getId();
        var this_file_name = file.getName();

        // submit BQ load job and log either side with details
        log_event_to_google_sheet_('Submitting Load Job for Pipeline: ' + pipeline_name + '. File Name: ' + this_file_name, LOG_TAB_NAME, DATE_FORMAT);
        var this_job = bq_load_csv_(bq_project_id, bq_dataset_id, bq_table_id, this_file_id, skip_leading_rows, source_format, write_disposition);
        log_event_to_google_sheet_('BigQuery Load Job Started for: ' + bq_project_id + ':' + bq_dataset_id + ':' + bq_table_id, LOG_TAB_NAME, DATE_FORMAT);
        log_event_to_google_sheet_('View Status: https://console.cloud.google.com/bigquery?page=jobs&project='+ bq_project_id, LOG_TAB_NAME, DATE_FORMAT);

        log_event_to_google_sheet_('Submitted Load Job for Pipeline: ' + pipeline_name + '. File Name: ' + this_file_name, LOG_TAB_NAME, DATE_FORMAT);

        // move file to processed folder
        var processed_folder = DriveApp.getFolderById(processed_drive_folder_id);
        file.moveTo(processed_folder);

        var notification_text = 'The pipeline ran successfully.';
        // send email
        send_email_(email_recipients, EMAIL_SUBJECT, pipeline_name, notification_text, EMAIL_FOOTER);

        log_event_to_google_sheet_('Emails sent to : ' + email_recipients, LOG_TAB_NAME, DATE_FORMAT);
    
      }

    } else {

      log_event_to_google_sheet_('Skipping Row. Pipeline Not Enabled: ' + pipeline_name, LOG_TAB_NAME, DATE_FORMAT);

    }
    
  } // end of loop to go through each row in turn **********************************
  
  // completed successfully
  return true;
  
  
}

function send_email_(email_recipients, email_subject, pipeline_name, notification_text, footer_text) {

    var templ = HtmlService
        .createTemplateFromFile('email');
    
      templ.pipelineName = pipeline_name;
      templ.notificationText = notification_text;
      templ.footerText = footer_text;

      templ.emailSubject = email_subject;
      
      var message = templ.evaluate().getContent();
      
      GmailApp.sendEmail(email_recipients, email_subject, notification_text, {
        htmlBody: message
      });


}

/**
 * Load a CSV into BigQuery
 */
function bq_load_csv_(project_id, dataset_id, table_id, csv_file_id, skip_leading_rows, source_format, write_disposition) {
  
  // Load CSV data from Drive and convert to the correct format for upload.
  var file = DriveApp.getFileById(csv_file_id);
  var data = file.getBlob().setContentType('application/octet-stream');

  // Create the data upload job.
  var my_job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: project_id,
          datasetId: dataset_id,
          tableId: table_id
        },
        skipLeadingRows: skip_leading_rows,
        sourceFormat: source_format,
        writeDisposition: write_disposition,
      }
    }
  };
  
  return load_job = BigQuery.Jobs.insert(my_job, project_id, data);
  
}

/**
 * 
 * Run this to process all the CSV files in the pending director.
 * 
*/

function get_pending_csv_files_(source_drive_folder_id) {
  var folder = DriveApp.getFolderById(source_drive_folder_id);
  var files = folder.getFiles();
  
  return files;

}

/** 
* Write log message to Google Sheet
*/

function log_event_to_google_sheet_(text_to_log, log_tab_name, date_format) {
  
  // get the user running the script
  var activeUserEmail = Session.getActiveUser().getEmail();
  
  // get the relevant spreadsheet to output log details
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var googleSheet = ss.getSheetByName(log_tab_name);
  
  // create and format a timestamp
  var now = new Date();
  var timeZone = ss.getSpreadsheetTimeZone();
  var niceDateTime = Utilities.formatDate(now, timeZone, date_format);
  
  // create array of data for pasting into log sheet
  var logData = [niceDateTime, activeUserEmail, text_to_log];
  
  // append details into next row of log sheet
  googleSheet.appendRow(logData);
  
}
