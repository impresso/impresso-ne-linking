# Notes

## Calling the AidaLight server from Python via Java

Launch the ssh tunnel to connect to the aidalight server running on dhlabsrv14

```bash
ssh -N -L 52365:localhost:52365 romanell@dhlabsrv14.epfl.ch
```

```bash
screen -dmS aida-gateway java -jar lib/aida-impresso.jar
```


```python
from py4j.java_gateway import JavaGateway
gateway = JavaGateway()

aida_server = gateway.entry_point.getAida()
test_sentence = "Monsiuer [[Nicolas de Largillière]] habite en [[Russie]] ou [[non so dove]]."
mentions = gateway.entry_point.findExtractedMentions(test_sentence)

annotations = aida_server.disambiguate(test_sentence, mentions, "fullSettings")

for mention in annotations.keySet():
    wikipedia_entity_id = f"http://en.wikipedia.org/wiki/{annotations.get(mention).getName()}"
    print(wikipedia_entity_id)
```


## Serialization format

s3://processed-canonical-data/entities/EXP/xxx.jsonl.bz2

```json
// confidences level ?
{
  "source": "s3://processed-canonical-data/ne_mentions/EXP/599.jsonl.bz2",
  "kb": "wikipedia",
  "sys_id": "aidalight",
  "cdt": "2019-05-28 ...",
  "sys_settings": "fullSettings",
  "entities": [
    {
      "mention_id": "EXP-1993-01-14-a-i0055@23:27",
      "type": "Location",
      "surface": "IRAK",
      "entityId" : "Iraq"
    },
    {
      "ciId": "EXP-1991-01-05-a-i0121@60:69",
      "type": "Location",
      "surface": "Neuchâtel",
      "entityId" : "Neuchâtel"
    }
  ]
}
```
