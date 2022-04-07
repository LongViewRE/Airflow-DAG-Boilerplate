# Airflow DAG Boilerplate 

This repository represents a boilerplate package for writing DAGs that are deployed using the Docker Operator. 

The package is structured in the following way: 

```
.github
  -workflows
    -deploy.yml
dags
  -__init__.py
  -dag.py
module
  -submodule
    -__init__.py
    -sync.py
  -__init__.py
  -__main__.py
Dockerfile
dockerpush.sh
requirements.txt
```

We have delimited elements of each page with the following characters to indicate they need to be updated:

```python

#### CHANGE
#

```



