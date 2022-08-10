<script runat=server>
    Platform.Load("core", "1.1.1");
    
      var url = 'https://test.salesforce.com/services/oauth2/token?';
     
      var client_id = '?';
      var client_secret = '?';
      var grant_type = 'password';
      var username = '?';
      var password = '?';
    
      url += 'client_id=' + client_id + '&';
      url += 'client_secret=' + client_secret + '&';
      url += 'grant_type=' + grant_type + '&';
      url += 'username=' + username + '&';
      url += 'password=' + password;
    
      var result, json, access_token, instance_url;
      try{
        result = HTTP.Post(url,"application/json",null,null); //in case of error this function goes to try catch
        json = Platform.Function.ParseJSON(result.Response[0]);
        access_token = json.access_token;
        instance_url = json.instance_url;
      }catch(error){
        result = HTTP.Post(url,"application/json",null,null);
        json = Platform.Function.ParseJSON(result.Response[0]);
        access_token = json.access_token;
        instance_url = json.instance_url;
      }
      
      Write('<br>Result: ' + json.access_token);
      Write('<br>Result: ' + json.instance_url);
     
      var logEndpoint = instance_url + '/services/data/v50.0/composite/tree/Marketing_Cloud_Activity_Log__c';
      Write('<br>logEndpoint: ' + logEndpoint);
    
      var logResult = '';
      
     
      try{
          Write("<br/>" + 'WSProxy start')
          var prox = new Script.Util.WSProxy();
          var deName = '9933EDC0-1598-42CD-929A-D82F13B9D741';
          var hasMoreRows = true;
          var reqId;
          var cols = ["Customer_Activity", "Customer_Id", "Timestamp", "Metadata", "Email", "Internal_Timestamp", "URL"];
    
          do {
            var records = [];
            var resp;
            if (!reqId) {
              resp = prox.retrieve("DataExtensionObject[" + deName + "]", cols);
            } else {
              resp = prox.getNextBatch("DataExtensionObject[" + deName + "]", reqId);
            }
            
            if (resp.Results) {
              hasMoreRows = resp.HasMoreRows || false;
              reqId = resp.RequestID;
              
              var counter = 0;
              for (var i = 0; i < resp.Results.length; i++) {
                var row = resp.Results[i];

                var customerActivity = row.Properties[0].Value;
                var customerId = row.Properties[1].Value;
                var timestamp = TreatAsContent("%%=FormatDate(\"" +row.Properties[2].Value + "\",\"yyyy-MM-dd'T'HH:mm:ss'Z'\")=%%");
                
                var metadata = row.Properties[3].Value;
                var email = row.Properties[4].Value;
                var internalTimestamp = row.Properties[5].Value;
                var url = row.Properties[6].Value;

                
                var attributes = {};
                attributes.type = "Marketing_Cloud_Activity_Log__c";
                attributes.referenceId = "ref" + i;

                var jsonInstance = {};
                jsonInstance.attributes = attributes;
                jsonInstance.Customer_Activity__c = customerActivity;
                jsonInstance.Email__c = email;
                jsonInstance.Salesforce_Record_Id__c = customerId;
                jsonInstance.Timestamp__c = timestamp;
                jsonInstance.Payload__c = metadata;
                jsonInstance.URL__c = url;

                records.push(jsonInstance);
                
                counter++;
                if(i == resp.Results.length - 1 || counter == 200){
                    try{
                        Write("<br/>" + new Date());
                    
                        var payload = '{"records":' + Stringify(records) + '}';

                        var req = new Script.Util.HttpRequest(logEndpoint);
                        req.emptyContentHandling = 0;
                        req.retries = 2;
                        req.continueOnError = true;
                        req.contentType = "application/json; charset=utf-8";
                        req.method = "POST";
                        req.postData = payload;
                        req.setHeader("Authorization", "Bearer " + access_token);
                        req.setHeader("Accept-Encoding","gzip, deflate, br");
                        req.setHeader("Accept", "*/*");
    
                        var resp = req.send();
                        Write("<br/>" + Platform.Function.ParseJSON(String(resp.content)));
                        Write("<br/>" + String(resp.content));
                    }catch(e){
                        Write("<br/>" + Stringify(e)); 
                    }finally{
                        records = [];
                        counter = 0;
                    }
                }
              }
            } else {
              hasMoreRows = false;
            }
          } while (hasMoreRows);
      }catch(error){
        Write("<br/>" + Stringify(error));
      }  
</script>
