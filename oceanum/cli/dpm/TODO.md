
# TODO list

## Commands

### Project

- [x] deploy [file] -f specfile -p [project_name] --org [org_name] --member [email]
- [x] validate [file]

- [x] list projects --search [search] --org [org_name] --member [email]
- [x] delete project -f [file] -p [project_name] --org [org_name]

- [ ] inspect project -f [file] -p [project_name] --org [org_name] --member [email] opens ArgoCD UI

# Route Commands

- [x] list routes
- [x] describe route [route_name]
- [x] update route thumbnail [route_name] -p [project_name] [thumbnail_file]
- [ ] update route description [route_name] -p [project_name] [description]
- [ ] update route active [route_name] -p [project_name] [active]
- [ ] update route display-name [route_name] -p [project_name] [display_name]
- [ ] inspect route [route_name] -p [project_name] 
opens ArgoCD UI


#### Secrets/Configmaps Commands

- [ ] list secrets
- [ ] describe secret [secret_name] -p [project_name]
- [ ] create secret [secret_name] -p [project_name] --key [key1] --value [value1] --key [key2] --value [value2]
- [ ] update secret data [secret_name] -p [project_name] key=value;key2=value2
- [ ] update secret remove [secret_name] -p [project_name] --key [key]
- [ ] update secret add [secret_name] -p [project_name] --key [key] --value [value]

#### Image Commands

- [ ] list images
- [ ] describe image [image_name]
- [ ] delete image [image_name] -p [project_name]
- [ ] create image [image_name] -p [project_name] --image [image_path] --registry [registry] --username [username] --password [password] --secret [secret_name] 
- [ ] update image [image_name] -p [project_name] --image [image_path] --registry [registry] --username [username] --password [password] --secret [secret_name]
- [ ] inspect service [service_name] -p [project_name]

#### Service Commands

- [ ] list services
- [ ] describe service [service_name]
- [ ] delete service [service_name] -p [project_name] --org [org_name] --member [email]
- [ ] create service [service_name] -p [project_name] --image [image_name] --port [port] --replicas [replicas] --env [env_vars] ...
- [ ] update service [service_name] -p [project_name]

#### Build Commands

- [ ] list builds
- [ ] describe build [build_name]
- [ ] delete build [build_name] -p [project_name]
- [ ] create build [build_name] -p [project_name] --image [image_name] --build-ref [build_name] --image-ref [image_ref]
- [ ] update build [build_name] -p [project_name]
- [ ] update build image [build_name] -p [project_name] --image [image_name] --build-ref [build_name] --image-ref [image_ref]

#### Stages Commands

- [ ] list stages
- [ ] describe stage [stage_name]
- [ ] delete stage [stage_name] -p [project_name]
- [ ] create stage [stage_name] -p [project_name]
- [ ] update stage active [active] -s [stage_name] -p [project_name]
- [ ] update stage remove service [service_name] -s [stage_name] -p [project_name]
- [ ] update stage add service [service_name] -s [stage_name] -p [project_name]
- [ ] update stage add pipeline [pipeline_name] -s [stage_name] -p [project_name]
- [ ] update stage remove pipeline [pipeline_name] -s [stage_name] -p [project_name]
- [ ] update stage ...

#### Pipeline Commands

- [ ] list pipelines
- [ ] describe pipeline [pipeline_name]
- [ ] delete pipeline [pipeline_name] -p [project_name]
- [ ] create pipeline [pipeline_name] -p [project_name]
- [ ] update pipeline trigger cron [pipeline_name] -p [project_name] --schedule [schedule] --suspend=True --stage [stage_name]
- [ ] inspect pipeline [pipeline_name] -p [project_name]
 opens Argo Workflows UI filtering by this pipeline

#### Tasks Commands

- [ ] list tasks
- [ ] describe task [task_name]
- [ ] delete task [task_name] -p [project_name]
- [ ] create task [task_name] -p [project_name] --image [image_name] --env [env_vars] ...
- [ ] update task [task_name] -p [project_name]
- [ ] update task image [task_name] -p [project_name] --image [image_name] --build-ref [build_name] --image-ref [image_ref]

