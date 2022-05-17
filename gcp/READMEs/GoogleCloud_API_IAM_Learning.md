# Google Cloud APIs and IAM

## Cloud APIs:
### How to List all APIs (*Gcloud Command):
```shell
gcloud services list
```

### How to Enable APIs (*Gcloud Command):
```shell
gcloud services enable dataflow.googleapis.com
```

### How to set GOOGLE_APPLICATION_CREDENTIALS:
```unix
#for Linux/mac-os
export GOOGLE_APPLICATION_CREDENTIALS="KEY_PATH"
```

```powershell
#for windows
#from powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="KEY_PATH"

#from command prompt(cmd)
set GOOGLE_APPLICATION_CREDENTIALS=KEY_PATH
```



## IAM:
### How to create Service Account or add new principle:
