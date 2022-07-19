import requests
import xml.etree.ElementTree as ET
import zipfile
import base64
import boto3
from io import BytesIO
import datetime
from datetime import date
from botocore.errorfactory import ClientError

def create_foldername(proj_title):
    """ returns file name without spaces """

    pq_proj_title = proj_title.replace(" ", "_")
    #print(f"folder to save: {pq_proj_title}")
    return pq_proj_title

def lambda_handler(event, context):
    """
    function pulls end date == previous date (end datetime seems to be always at 1159)
    """
  
    #Endpoint information for prod
    url = "https://feedback.sim.edu.sg/simWS/Dashboard.asmx?WSDL"
    ApiKey = "api-key-here"
    
    headers = {
        'Content-Type': 'text/xml',
        'cache-control': 'no-cache',
    }
    
    # Get Projects Metadata
    payload = """
    <soapenv:Envelope
        xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
        xmlns:tem="http://tempuri.org/">
       <soapenv:Header>
          <tem:Message>
             <tem:Value></tem:Value>
          </tem:Message>
          <tem:APIKeyHeader>
             <!--Optional:-->
             <tem:Value>"""+ApiKey+"""</tem:Value>
          </tem:APIKeyHeader>
       </soapenv:Header>
       <soapenv:Body>
          <tem:GetAllProjectMetaData/>
       </soapenv:Body>
    </soapenv:Envelope>""";
    Projects = requests.post(url,data=payload,headers=headers)
    myroot = ET.fromstring(Projects.text)
    
    ####### Identifying Records to Ingest #######
    
    #setting yesterday's date
    today = date.today()
    yesterday = today - datetime.timedelta(days=1)
    yesterday.strftime("%Y-%m-%d")
   
    for x in myroot[1][0][0]:
        if x[2].text !=None:
            projectStartDate = datetime.datetime.strptime(x[2].text, '%m/%d/%Y %I:%M:%S %p').date()
            #projectStartDate = datetime.datetime.strptime(str(projectStartDate),'%Y-%m-%d %I:%M:%S %p').date()
        else:
            continue
        if x[3].text !=None:
            projectEndDate = datetime.datetime.strptime(x[3].text, '%m/%d/%Y %I:%M:%S %p').date()
            #projectEndDate =datetime.datetime.strptime(str(projectEndDate),'%Y-%m-%d %I:%M:%S %p').date()
        else:
            continue
    
        if projectEndDate != yesterday:
            continue
        
        if x[4].text != 'expired':
            continue
        
        else:
            projectID = x[0].text
            projectTitle = x[1].text
            projectstatus = x[4].text

            
    ####### Pulling the identified record #######
            
        #print(now)
        payload = """
        <soapenv:Envelope
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:tem="http://tempuri.org/">
            <soapenv:Header>
                <tem:Message>
                    <tem:Value></tem:Value>
                </tem:Message>
                <tem:APIKeyHeader>
                    <tem:Value>"""+ApiKey+"""</tem:Value>
                </tem:APIKeyHeader>
            </soapenv:Header>
            <soapenv:Body>
                <tem:GetResponse4>
                    <tem:ProjectID>"""+projectID+"""</tem:ProjectID>
                    <tem:OutputType>score</tem:OutputType>
                    <tem:currentLanguage>en-US</tem:currentLanguage>
                    <tem:DateFormatStr>MM/dd/yy hh:mm tt</tem:DateFormatStr>
                    <tem:FilloutDateFrom></tem:FilloutDateFrom>
                    <tem:FilloutDateTo></tem:FilloutDateTo>
                    <tem:IncludeDemographic>all</tem:IncludeDemographic>
                    <tem:UseQuestionIdentifier></tem:UseQuestionIdentifier>
                    <tem:QuestionFilter></tem:QuestionFilter>
                    <tem:ExportQP></tem:ExportQP>
                    <tem:useLatestQP></tem:useLatestQP>
                    <tem:ExportQbank></tem:ExportQbank>
                    <tem:ExportToSeparateFiles></tem:ExportToSeparateFiles>
                </tem:GetResponse4>
            </soapenv:Body>
        </soapenv:Envelope>"""
    
    
        response=requests.post(url,data=payload,headers=headers)
        assert response.status_code == 200, "Requests did not go through"
    
        root = ET.fromstring(response.content)
        #Get <GetResponse4Result>
        data = root[1][0][0].text
       
        decode_b64 = base64.b64decode(data)
        # filepath = 'D:\Temp\SIM\\'+projectID+'.zip'
         #out_file = open(filepath, 'wb')
         #out_file.write(decode_b64)
         #out_file.close()
        projectStartDate = str(projectStartDate)
        projectEndDate=str(projectEndDate)
        print('Project Id ',projectID)
        
        projectTitle = create_foldername(projectTitle)
        
        print('Project Title ',projectTitle)
        print(' Start date', projectStartDate)
        print(' End date', projectEndDate)
        print(' projectstatus ', projectstatus)
        client = boto3.client('s3')
        resource=boto3.resource('s3')
        
        keyval = f"smile_data/zipped/{projectTitle}({projectStartDate}-{projectEndDate})"
        result = client.list_objects(Bucket="sim-raw-bucket", Prefix=keyval )
        
        if 'Contents' in result:
            print('Already there ')
        else:
            print("inside loading")

            response = client.put_object(Bucket="sim-raw-bucket", Key=keyval, Body=decode_b64)
            zip_obj = resource.Object(bucket_name="sim-raw-bucket", key=f"smile_data/zipped/"+projectTitle+ "("+ projectStartDate +"-"+ projectEndDate +")")
         
            buffer = BytesIO(zip_obj.get()["Body"].read()) 
            z = zipfile.ZipFile(buffer) 
            
        # for each file within the zip 
            for filename in z.namelist(): 
                file_info = z.getinfo(filename)   
              
                # Now copy the files to the 'unzipped' S3 folder 
                response = client.put_object( Body=z.open(filename).read(), 
                                              Bucket="sim-raw-bucket", 
                                              Key=f"smile_data/unzipped/"+projectTitle+ "("+ projectStartDate +"-"+ projectEndDate +")"+"/"+filename
                                            ) 
            
