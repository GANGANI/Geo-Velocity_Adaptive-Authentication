
// Specify the Siddhi application name.
var siddhiApplication = 'GeoVelocity-Login';
// Specify the Siddhi input stream name.
var siddhiInputStream = 'InputStream';

function onInitialRequest(context) {
    executeStep(1, {
        onSuccess: function (context) {
            var username = context.steps[1].subject.username;
		  	var loginTime = String(Date.now());
		  	var loginIp = context.request.ip;
		  	var currentLocation = checkLocation("72.229.28.185").split(" ");
		  	var latitude = currentLocation[0];
		  	var longitude = currentLocation[1];
		  	Log.info('username '+username+' loginTime '+loginTime+' latitude '+latitude+' longitude '+longitude);
            callAnalytics({'Application':siddhiApplication,'InputStream':siddhiInputStream},{"events":{"event": {'username':username, 'loginTime':loginTime, 'latitude':latitude, 'longitude':longitude}}} , {
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
