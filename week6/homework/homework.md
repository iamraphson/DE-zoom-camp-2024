# Week 6 homework answer

[Assignment](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2024/06-streaming/homework.md)

### Question 1:
what is the Redpanda version?

**To get the version** 
- run `docker-compose up -d` in the current working directory.
- When the 1st step is done, run `docker exec -it redpanda-1 bash` to enter into the container bash
- run `rpk version` in the container bash.

**What's the output?**
`v22.3.5 (rev 28b2443)`


### Question 2:
To create a topic.

**To create a topic**
In the container bash, run `rpk topic create test-topic`

**What's the output?**
```bash
TOPIC       STATUS
test-topic  OK
```

### Question 3:
Connecting to the Kafka server

[Jupyter notebook for Question 3](Question_3.ipynb)

### Question 4:
Sending data to the stream

**The answer is `Sending the messages`**

[Jupyter notebook for Question 4](Question_4.ipynb)

### Question 5:
Sending the Trip Data

**What's the output?**
took 30.69 seconds

[Jupyter notebook for Question 5](Question_5.ipynb)

### Question 6:
Parsing the data

**What's the output?**
```python
DataFrame[
    lpep_pickup_datetime: string,
    lpep_dropoff_datetime: string, 
    PULocationID: int, 
    DOLocationID: int, 
    passenger_count: double, 
    trip_distance: double, 
    tip_amount: double
]

```
[Jupyter notebook for Question 6](Question_6.ipynb)

### Question 7:
Most popular destination

**What's the output?**
`74`
```
+------------------------------------------+------------+-----+
|window                                    |DOLocationID|count|
+------------------------------------------+------------+-----+
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|74          |53223|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|42          |47826|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|41          |42183|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|75          |38520|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|129         |35790|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|7           |34599|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|166         |32535|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|236         |23739|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|223         |22626|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|238         |21954|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|82          |21876|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|181         |21846|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|95          |21732|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|244         |20199|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|61          |19818|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|116         |19017|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|138         |18432|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|97          |18150|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|49          |15663|
|{2024-04-01 16:30:00, 2024-04-01 16:35:00}|151         |15459|
+------------------------------------------+------------+-----+
```
[Jupyter notebook for Question 7](Question_7.ipynb)