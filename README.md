# estela-airflow-etl

This project manages an Apache Airflow in deployment with estela using Helm.

## Commands
You need to go to the `k8s_installation` dir.

### Build the Docker Image

```bash
make build
```
Builds the Airflow Docker image and tags it.  
Default values:
- `PLATFORM=linux/amd64`
- `IMAGE_NAME=airflow`
- `TAG=latest`
- `REGISTRY=localhost:5001`

### Push the Docker Image

```bash
make push
```
Pushes the Docker image to the registry.

### Install Airflow

```bash
make install
```
Installs Airflow using Helm. To configure connections, specify the Airflow Docker image, manage resource limits, and more, modify the `override.yaml` file. The file includes detailed instructions on how to apply these settings and also allows you to set Git sync credentials.

Default values:
- `RELEASE_NAME=airflow`
- `NAMESPACE=airflow`
- `SSH_KEY=~/.ssh/dags_ssh`

### Uninstall Airflow

```bash
make uninstall
```
Removes the Airflow deployment from the cluster.

### Upgrade Airflow

```bash
make upgrade
```
If you make changes on values.yaml you should upgrade airflow using this command.