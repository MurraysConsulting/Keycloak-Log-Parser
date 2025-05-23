# Keycloak-Log-Parser
A parser to pull out login, logout, refresh token data, create user mappings for output.

The CSL commands to get the raw data is:

SERVICE_QUERIES[subhub]='fields @timestamp, @message, @logStream
| filter @message like "roboticknight_keycloak"
| filter @message like "redirect_uri=https://subhub.roboticknight.cybercom.mil/"
| filter @message like "clientId=subhub"
| sort @timestamp desc
| limit 10000'

SERVICE_QUERIES[misp]='fields @timestamp, @message, @logStream
| filter @message like "roboticknight_keycloak"
| filter @message like "redirect_uri=https://misp.roboticknight.cybercom.mil/users/login"
| filter @message like "clientId=misp"
| sort @timestamp desc
| limit 10000'

SERVICE_QUERIES[opensearch]='fields @timestamp, @message, @logStream
| filter @message like "roboticknight_keycloak"
| filter @message like "clientId=https://metadata.roboticknight.cybercom.mil"
| filter @message like "userId="
| sort @timestamp desc
| limit 10000'

Set the output to go into a list with a dictionary wrapper aound each entry.

Note that you may have to adjust your requests based on number of entries as 10k is a hard limit for AWS CONSOLE CloudWatch entries.

Set the clientID to the relevant items, can parse your Keycloak config to see what clientIDs are available to parse.


Working with the team to add some more depth to the above queries, as well as I will clean up my jupyter notebook code a bit to make it more readable as I didn't know how github would handle it.

If there are questions or comments please let me know at: john@murrayscg.com

Thanks.
