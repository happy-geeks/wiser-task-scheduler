# Wiser Task Scheduler (WTS)
The Wiser Task scheduler (WTS in short) is a .net core service which can be used to run specific tasks in various time-schedules. The WTS is configured with JSON-based configuration files. These configuration files can also be read directly from a Wiser account if Wiser is in place.

## Prerequisites
[.NET Desktop runtime 7](https://dotnet.microsoft.com/en-us/download/dotnet/7.0) (x64) needs to be installed on the server

## Installation of the WTS on a Windows server
0. Check the Prerequisites above
1. Build the project (**not** as single file) as described in the Build section below
2. Copy all build files to the desired folder on the server
3. Setup the Appsettings.json file (see documentation)
4. Open the command prompt (CMD) or PowerShell with administrator rights
5. Run this command: `sc.exe create "<Service name>" binpath="<Service exe location>"`<br>- change `<Service name>` with a name you prefer ('WTS Auto Updater' for example)<br>- change `<Service exe location>` to the location of the .exe file (for example: 'c:\WTSAutoUpdater\autoupdater.exe')
6. Open the services panel in Windows (you can run 'msc' via the Start menu)
7. Find the newly created service and open its properties
8. Change the startup type to 'automatic (delayed start)'
9. Start the service
10. Wait untill the service is started
11. check the log files for a succesfull startup

## Installation of the auto updater (optional)
The auto updater is a tool embedded into the WTS project to automatically update the WTS whenever there is a new release. This can come in handy if you have multiple servers running the WTS. Only if something has gone wrong or there is a newer version with breaking changes is a notification given because a manual action is required. Updates take place at midnight, local server time. In order for this to work the auto updater also needs to be installed on the server. Follow the steps below (these steps are almost the same as the steps above, only the name and location of this service are difzferent);
0. Check the Prerequisites above
1. Build the project (**not** as single file) as described in the Build section below
2. Copy all build files to the desired folder on the server
3. Setup the Appsettings.json file (see documentation)
4. Open the command prompt (CMD) or PowerShell with administrator rights
5. Run this command: `sc.exe create "<Service name>" binpath="<Service exe location>"`<br>- change `<Service name>` with a name you prefer ('WiserTaskScheduler' for example)<br>- change `<Service exe location>` to the location of the .exe file (for example: 'c:\wts\WiserTaskScheduler.exe')
6. Open the services panel in Windows (you can run 'msc' via the Start menu)
7. Find the newly created service and open its properties
8. Change the startup type to 'automatic (delayed start)'
9. Start the service
10. Wait untill the service is started
11. check the log files for a succesfull startup

## Manual updating the WTS
If the auto updater is not used or if the auto updater has indicated that a manual action is required, the steps below can be followed to update the WTS to a new version:
1. Create a new build according to section "Create new build";
2. Open the task manager and go to the services tab;
3. Find the service and stop it;
4. Copy all published files (excluding appsettings) to the desired server/folder;
5. Start the service;
6. Wait until it is set to active 
7. check the logs to see if it started correctly.

## De-installing the service (WTS and/or Auto Updater)
1. Open the command prompt (CMD) or PowerShell with administrator rights
2. Run this command: `sc.exe delete "<Service naam>"`<br>- change `<Service name>` with the correct name of the service
3. If you want to completely remove the WTS or the Auto Updater then also remove the files from the server 

## Creating a new release
1. Open `WiserTaskScheduler.csproj` in a text editor and increase the version number at "Version", "AssemblyVersion" and "FileVersion";
2. Start the project in your favorite SDE;
3. Right click on the project and click "Publish";
4. Click "Publish WiserTaskScheduler to folder" or "Publish AutoUpdater to folder" (depending on which project to create a new build);
5. The application will be published in the project folder "bin\Release\net7.0\win-x64\publish\";
6. Check that all DLL files are present in the publish folder, about 400 files should be present. If these are missing, the application is built as a "Single file" and cannot be started.
7. Place all files **EXCLUDING** appsettings in a zip file called "version{version}.zip" (e.g. version1.3.6.0.zip) and upload it to the server that hosts the WTS versions;
8. Update the "version.json" file in the "Update" folder;
9. Merge to main;
10. Create a new release in Github with the tag the version numer (e.g. v1.0.0.0). Auto generate the notes to list all changes for the new version. **Only for WTS releases, not for the auto updater.**

## Setup secrets<a name="setup-secrets"></a>
1. Create a file named `wts-appsettings-secrets.json` somewhere outside of the project directory.
1. Open `appSettings.[Environment].json` in the project and save the directory to the secrets in the property `Wts.SecretsBaseDirectory`. When running Wiser Task Scheduler locally on your PC, you need the file `appSettings.Development.json`. Please note that this directory should always end with a slash. Example: `Z:\AppSettings\WiserDemo\`.
1. The `wts-appsettings-secrets.json` file should look like this:
### Example
```json
{
	"GCL": {
		"DefaultEncryptionKey": "", // Optional: The encryption key to use to encrypt/decrypt OAuth information in the database. Only needed if one is present.
		"ConnectionString": "", // Mandatory: The connectionstring to the database to write logs and service information.
		"SmtpSettings": null // Optional: Information to send emails if one is provided in "ServiceFailedNotificationEmails"
	},
	"Wts": {
		"Wiser": { // Optional: Only needed if no local configuration has been provided.
			"Username": "", // Mandatory: The username of a Wiser user to retrieve the configurations.
			"Password": "", // Mandatory: The password of the Wiser user.
			"Subdomain": "", // Mandatory: The subdomain on which the Wiser customer is running.
			"WiserApiUrl": "", // Mandatory: The URL to the Wiser API that needs to be used.
			"ClientId": "wiser",
			"ClientSecret" : "", // Mandatory: The client secret the API is expecting.
			"TestEnvironment": false,
			"ConfigurationPath": "settings" // Mandatory: The path to the folder that contains the services this WTS needs to execute.
		},
		"SlackSettings": null, // Optional: Settings to send error messages to a Slack channel.
		"ServiceFailedNotificationEmails": "", // Optional: Emailsadresses, semicolon splitted, to notify if errors occured outside of the runs.
		"Credentials": { // Optional: Key value pair to use as replacements in configurations using [{Credential:<key>}]
			"ApiKey": "ABCD1234",
			"DatabaseUser": "User",
			"DatabasePassword": "P@55w0rd"
		}
	}
}
```
