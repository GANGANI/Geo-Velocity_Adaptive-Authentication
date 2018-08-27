# Geo-Velocity_Adaptive-Authentication
## Prerequisites
-   [Maven](https://maven.apache.org/download.cgi)
-   [Java](http://www.oracle.com/technetwork/java/javase/downloads)
-   [WSO2 Identity Server](https://wso2.com/identity-and-access-management)
-   [Apache Tomcat server](https://tomcat.apache.org/download-80.cgi)
-  [WSO2 StreamProcessor](https://wso2.com/analytics-and-stream-processing/)
-   Get a clone or download source from [GeoVelocity-Functions](https://maven.apache.org/download.cgi)  repository and build it by running the Maven command "mvn clean install" from within the distribution directory. Then copy the jar file in the target directory to the <IS_HOME>/repository/components/dropins folder

  ## Building the source

  - Get a clone or download source from this repository

  ## Deploying the Siddhi application
  1. Copy the GeoVelocity-Login.siddhi file.
  2. Place it in the <SP_HOME>/deployment/siddhi-files directory. 
  3. Navigate to <SP_HOME>/bin and start WSO2 Stream Processor in a Worker profile.
  -For Windows : worker.bat  
  -For linux worker.sh

  ## Configuring the analytics engine in WSO2 IS
1. Start the WSO2 Identity Server and log in to the management console using admin/admin credentials. 
2. Click Resident under Identity Providers and expand Adaptive Authentication>Analytics Engine
3. Configure the following properties accordingly.
4. An HTTP connection is used to communicate between WSO2 IS and WSO2 SP. Therefore, you must add the certificate of WSO2 SP to WSO2 IS. Follow the steps given below to import the certificate from WSO2 SP to WSO2 IS. This example uses the default keystores and certificates. 

    a. Navigate to the <SP_HOME>/resources/security     directory on a new terminal window and run the     following command. 
    ```sh
    keytool -export -alias wso2carbon -keystore wso2carbon.jks -file sp.pem
    ```
    b. Navigate to the <IS_HOME>/repository/resources/security directory and run the following command.
    ```sh
    keytool -import -alias certalias -file <SP_HOME>/resources/security/sp.pem -keystore client-truststore.jks -storepass wso2carbon
    ```

## Getting started
#### Method 01
To get the geo-velocity among two login locations, You can execute the following two CURL commands seperatly in two terminals.
```sh
curl -X POST  [https://localhost:8280/GeoVelocity-Login/InputStream](https://localhost:8280/GeoVelocity-Login/InputStream)  -H 'Accept: application/json' -H 'Content-Type: application/json' -d '{
"events":{
"event": {
"loginTime": "1535081659",
"latitude": "40.7834",
"username": "gaga",
"longitude": "73.9662"
}
}
}' -kv -u admin:admin

curl -X POST  [https://localhost:8280/GeoVelocity-Login/InputStream](https://localhost:8280/GeoVelocity-Login/InputStream)  -H 'Accept: application/json' -H 'Content-Type: application/json' -d '{
"events":{
"event": {
"loginTime": "1535042561",
"latitude": "36.778259",
"username": "gaga",
"longitude": "-119.417931"
}
}
}' -kv -u admin:admin
```
#### Method 02
  1. Start the wso2 identity server.
  2. Create a new service provider in Identity Server with the name “saml2-web-app-dispatch.com”. 
  3. You can find more information about creating new service provider [here](https://docs.wso2.com/display/IS560/Adding+and+Configuring+a+Service+Provider). 
4.  under Inbound Authentication Configuration, create a new SAML2 Web SSO configuration with following properties. 
					- Issuer - saml2-web-app-dispatch.com
					- Assertion Consumer URLs - http://localhost.com:8080/saml2-web-app-dispatch.com/consumer 
					- Keep the other default settings as it is and save the configuration.
5. Add two authentication steps .
6. Copy the content of the Adaptive_Authentication_Script.js file to the script based adaptive authentication or update script as follows .
```sh
// Specify the Siddhi application name.
var siddhiApplication = 'GeoVelocity-Login';
// Specify the Siddhi input stream name.
var siddhiInputStream = 'InputStream';

function onInitialRequest(context) {
    executeStep(1, {
        onSuccess: function (context) {
            var username = context.steps[1].subject.username;
		  
		  	//Get login time.
		  	var loginTime = String(Date.now());
		  
		  	//Get login Ip
		  	var loginIp = context.request.ip;
		  
		  	//Get location details.
		  	var currentLocation = checkLocation("72.229.28.185").split(" ");
		  	var latitude = currentLocation[0];
		  	var longitude = currentLocation[1];
		  	
            callAnalytics({'Application':siddhiApplication,'InputStream':siddhiInputStream}, {'username':username, 'loginTime':loginTime, 'latitude':latitude, 'longitude':longitude} , {
                onSuccess : function(context, data) {
                    Log.info('--------------- Received velocity value: ' + data.event.velocity);
                    if (data.event.velocity > 100) {
                        executeStep(2);
                    }
                }, onFail : function(context, data) {
                    Log.info('--------------- Failed to call analytics engine');
                    executeStep(2);
                }
            });
        }
    });
}
```
7. Start the tomcat server.
8. Try out single sign on flow.

Note:-Please add the host names used for the applications to your etc/hosts file. You can find the needed host names through the property files. Addition to that, use the call back urls in the property files when configuring inbound protocols for each service providers

