package com.amazonaws.lambda.cloudwatchevent;



import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.CloudWatchLogsEvent;

import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.CreateSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.CreateSecretResponse;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.ResourceNotFoundException;
import software.amazon.awssdk.services.secretsmanager.model.UpdateSecretRequest;
import software.amazon.awssdk.services.secretsmanager.model.UpdateSecretResponse;


public class LambdaFunctionHandler implements RequestHandler<CloudWatchLogsEvent, String> {

	@Override
	public String handleRequest(CloudWatchLogsEvent input, Context context) {
		// TODO Auto-generated method stub
		
		String JWKS_URL =System.getenv("JWKS_URL");
		String TOKEN =System.getenv("TOKEN");
		String SECRET_VALUE_ID=System.getenv("SECRET_VALUE_ID");
		
		try {
			HttpRequest httpRequest=HttpRequest.newBuilder()
								.uri(new URI(JWKS_URL))
								.GET()
								.build();
			HttpClient httpClient=HttpClient.newBuilder().build();
			
			//Calling jwks API 
			HttpResponse<String> httpResponse =httpClient.send(httpRequest, BodyHandlers.ofString());
			context.getLogger().log("Response from okta :"+httpResponse.body().toString());
			return createOrUpdateSecret(SECRET_VALUE_ID, context, httpResponse.body());
		}catch (Exception e) {
			context.getLogger().log("Exception while Getting Keys from Otka "+e);
			return "Exception while Getting Keys "+e.getMessage();
		}
		
	}
	
	private String createOrUpdateSecret(String secretId,Context context,String secString) {
		SecretsManagerClient secretsManagerClient=SecretsManagerClient.builder().build();	
		try {
				GetSecretValueRequest getSecretValueRequest=GetSecretValueRequest.builder()
				    		.secretId(secretId)
				    		.build();
		
				context.getLogger().log("Getting Existing Secret :");
				UpdateSecretRequest updateSecretRequest = UpdateSecretRequest.builder().secretId(secretId)
						.secretString(secString)
						.build();
				GetSecretValueResponse getSecretValueResponse =secretsManagerClient.getSecretValue(getSecretValueRequest);
				context.getLogger().log("Secret Exists : ARN :- "+getSecretValueResponse.arn());
			    UpdateSecretResponse updateSecretResponse=secretsManagerClient.updateSecret(updateSecretRequest);
				return updateSecretResponse.arn();
				
				
			}catch (ResourceNotFoundException e) {
				context.getLogger().log("Secret doesn't exists :- Creating a new one -");
				CreateSecretRequest createSecretRequest=CreateSecretRequest.builder().secretString(secretId)
						.name(secretId)
						.build();
				
				CreateSecretResponse createSecretResponse=secretsManagerClient.createSecret(createSecretRequest);
				return createSecretResponse.arn();
			}catch (Exception e) {
				context.getLogger().log("Exception while Creating Or Updating Secret Key : "+e);
				return "Exception while Creating Or Updating Secret Key :-" + e.getMessage();
			}
    
	}
}
