#Created By: Indranil Pal
#Created Date: 11-11-2021


#-------------------------------------------------------------------------------------------
#Dynamic Filename Generated
_now=$(date +"%m_%d_%Y")
_filename="output_$_now.csv"
#echo "$_filename"

#Delete the file if exists
#-f checks if it's a regular file and -e checks if the file exist
#if [ -fe $_filename ]; then rm $_filename; fi 
if [ $( ls $_filename ) ]; then rm $_filename; fi 
#-------------------------------------------------------------------------------------------

echo "------------------------------------------------------------------------------------" >> $_filename
for project in  $(gcloud projects list --format="csv[no-heading](projectId)") 
do
  echo "Script is running...Please wait..."
  gcloud config set project $project
  _account=`gcloud info --format="value(config.account)"`
  echo "ProjectId:  $project" >> $_filename
  echo "Logged in by: $_account" >> $_filename  
  echo "------------------------------------------------------------------------------------" >> $_filename
  
  #gcloud alpha billing accounts projects list
  
  for bl in $(gcloud beta billing accounts list --format="csv[no-heading, separator=';'](ACCOUNT_ID,OPEN,MASTER_ACCOUNT_ID)") 
  do
    echo "    -> Billing-accounts: $bl" >> $_filename
  done
  echo "------------------------------------------------------------------------------------" >> $_filename
  
  for sa in $(gcloud beta iam service-accounts list --project $project --format="csv[no-heading](email)") 
  do
    echo "    -> service-accounts: $sa" >> $_filename
  done
  echo "------------------------------------------------------------------------------------" >> $_filename
  
  IFS=$'\n'
  for sl in $(gcloud services list --enabled --project $project --format="csv[no-heading, separator=';'](config.name,config.title)") 
  do
	IFS=';' read -r -a slArray<<< "$sl"
	if [[ "${slArray[0]}" == "composer.googleapis.com" ]]; then _serviceComposer="${slArray[0]}"; fi
    echo "    -> services-enabled: $sl" >> $_filename
  done
  #_output=`gcloud services list --enabled --project $project --format="csv[no-heading, separator=';'](config.name,config.title,state)"`
  #echo "$_output" >> $_filename  
  
  echo "------------------------------------------------------------------------------------" >> $_filename
  for subnet in $(gcloud compute networks subnets list --format="csv[no-heading, separator=';'](network:label='VPC',REGION,name:label='Subnet',RANGE)")
  do
    IFS=';' read -r -a subnetArray<<< "$subnet"
	_region="${subnetArray[1]}"
    echo "    -> subnet: $subnet" >> $_filename
	if [[ "$_region" == "us-central1" && "_serviceComposer" == "composer.googleapis.com" ]];
	#if [[ "$_region" == "us-central1" ]];
	then
	  for composer in $(gcloud composer environments list --locations $_region --format="csv[no-heading, separator=';'](name:label='composer_env',LOCATION,state,create_time)")
	  do
	    echo "      -> composer-env: $composer" >> $_filename
	  done;
	fi
  done
  echo "------------------------------------------------------------------------------------" >> $_filename
  
  for firewall in $(gcloud compute firewall-rules list --format="csv[no-heading, separator=';'](NAME,NETWORK,DIRECTION,PRIORITY,ALLOW,DENY,DISABLED)")
  do
    echo "    -> firewall: $firewall" >> $_filename
  done
  echo "------------------------------------------------------------------------------------" >> $_filename
  
  for vm in $(gcloud compute instances list --format="csv[no-heading, separator=';'](name,zone,status)")
  do
    echo "    -> compute-instances: $vm" >> $_filename
  done
  echo "------------------------------------------------------------------------------------" >> $_filename
  
  for gcs in $(gsutil ls)
  do
    echo "    -> GCS Bucket: $gcs" >> $_filename
  done
  echo "------------------------------------------------------------------------------------" >> $_filename
  
  #for gcs in $()
  #do
  #  echo "    -> cloud-storage: $gcs" >> $_filename
  #done
  echo "------------------------------------------------------------------------------------" >> $_filename
done
echo "------------------------------------------------------------------------------------" >> $_filename

echo "Thanks, output file generated: $_filename"