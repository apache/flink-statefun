# Apache Stateful Functions - Python SDK Walkthrough

## Setup

* Create a virtual env

```
python3 -m venv venv
source venv/bin/activate   
```

* Install the requirements 

```
pip3 install -r requirements.txt
```

If you are building from source, then first build the 
distribution (via calling `statefun-python-sdk/build-distribution.sh`)
then copy `statefun-python-sdk/dist/apache_flink_statefun-<version>-py3-none-any.whl` here and
run 

```
pip3 install apache_flink_statefun-<version>-py3-none-any.whl
```

## Examples

* Checkout the walkthrough examples at [walkthrough.py](walkthrough.py)
* To invoke one of the example functions, and observe its result, run:
```
python3 walkthrough.py
```

And from another terminal run:
```
python3 run-example.py <example name>
```

e.g.

```
python3 run-example.py walkthrough/hello
```
