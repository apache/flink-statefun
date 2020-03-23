# Apache Stateful Functions - Walkthrough

## Setup

* Create a virtual env

```
python3 -m venv venv
source venv/bin/activate   
```

* Install the requirements 

```
pip3 install requirements.txt
```

If you are building from source, then first build the 
distribution (via calling `statefun-python-sdk/build-distribution.sh`)
then copy `statefun-python-sdk/dist/apache_flink_statefun-1.1_SNAPSHOT-py3-none-any.whl` here and
run 

```
pip3 install apache_flink_statefun-1.1_SNAPSHOT-py3-none-any.whl
```

## Examples

* [Hello world](00hello.py) 
* [Packing and unpacking messages](01any.py)
* [State access](02state.py)
* [Sending messages](03context.py)


