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
    
      var result = HTTP.Post(url,"application/json",null,null);
      var json = Platform.Function.ParseJSON(result.Response[0]);
      var access_token = json.access_token;
      var instance_url = json.instance_url;
      
      Write('<br>Result: ' + json.access_token);
      Write('<br>Result: ' + json.instance_url);
     
      var logEndpoint = instance_url + '/services/data/v50.0/composite/tree/Marketing_Cloud_Activity_Log__c';
      Write('<br>logEndpoint: ' + logEndpoint);
    
      var logResult = '';
      
      var records = [];
      try{
          Write("<br/>" + 'WSProxy start')
          var prox = new Script.Util.WSProxy();
          var deName = '?';
          var hasMoreRows = true;
          var reqId;
          var cols = ["Customer_Activity", "Customer_Id", "Timestamp", "Metadata", "Email", "Internal_Timestamp", "URL"];
    
          do {
            var resp;
            if (!reqId) {
              resp = prox.retrieve("DataExtensionObject[" + deName + "]", cols);
            } else {
              resp = prox.getNextBatch("DataExtensionObject[" + deName + "]", reqId);
            }
            
            if (resp.Results) {
              hasMoreRows = resp.HasMoreRows || false;
              reqId = resp.RequestID;
              
              for (var i = 0; i < resp.Results.length; i++) {
                
                var row = resp.Results[i];

                var customerActivity = row.Properties[0].Value;
                var customerId = row.Properties[1].Value;
                var timestamp = TreatAsContent("%%=FormatDate(\"" +row.Properties[2].Value + "\",\"yyyy-MM-dd'T'HH:mm:ss'Z'\")=%%");
                
                var metadata = row.Properties[3].Value;
                var email = row.Properties[4].Value;
                var internalTimestamp = row.Properties[5].Value;
                var url = row.Properties[6].Value;

                // var arrayRecord = '{"attributes":{"type":"Marketing_Cloud_Activity_Log__c","referenceId":"ref' + i + '"},';
                // arrayRecord += '"Customer_Activity__c":"' + customerActivity + '",';
                // arrayRecord += '"Email__c":"' + email + '",';
                // arrayRecord += '"Salesforce_Record_Id__c":"' + customerId + '",';
                // arrayRecord += '"Timestamp__c":"' + timestamp + '",';
                // metadata = TreatAsContent("%%=Replace('" + metadata + "','\"','\\\"')=%%");
                // arrayRecord += '"Payload__c":"' + metadata + '",';
                // arrayRecord += '"URL__c":"' + url + '"}';

                // records.push(arrayRecord);

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
              }

            } else {
              hasMoreRows = false;
            }
          } while (hasMoreRows);

          Write("<br/>" + records.length);
          Write("<br/>" + 'hmm');
          var subArray = [];
          var payload = '';

          for(var i = 0 ; i < records.length; i++){
            subArray.push(records[i]);
            if( 
                ( (i != 0 && i % 199 == 0) || i == (records.length - 1) ) 
                || 
                (records.length == 1 && i == 0) 
            ){
                Write("<br/>" + i);
                try{
                    Write("<br/>" + new Date());
                    // payload = '{"records":[';
                    // for(var j = 0 ; j < subArray.length; j++){
                    //     payload += subArray[j] + ",";
                    // }

                    // var payload = payload.substring(0, payload.length - 1);
                    // payload += ']}';
                    var payload = '{"records":' + Stringify(subArray) + '}';

                    //Write("<br/>" + payload);

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
                    subArray = [];
                }

                if(i>600){
                    break;
                }
            }
          }
      }catch(error){
        Write("<br/>" + Stringify(error));
      }
      
    </script>
