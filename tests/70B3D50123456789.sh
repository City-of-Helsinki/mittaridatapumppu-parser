curl -X POST \
  'http://localhost:8000/digita/v2?token=abcd1234&LrnDevEui=70B3D57050011422&LrnFPort=2&LrnInfos=TWA_100002581.57949.AS-1-556889314' \
  -H 'Accept: */*' \
  -H 'Content-Type: application/json' \
  -H 'Connection: close' \
  -H 'Host: endpoint.example.org' \
  -H 'User-Agent: ACTILITY-LRCLRN-DEVICE-AGENT/1.0' \
  -H 'X-Forwarded-For: 52.16.83.187' \
  -H 'X-Forwarded-Proto: https' \
  -H 'X-Real-Ip: 52.16.83.187' \
  -d '{
    "DevEUI_uplink": {
        "Time": "2022-02-24T16:23:17.468+00:00",
        "DevEUI": "70B3D57050011422",
        "FPort": 20,
        "FCntUp": 3866,
        "ADRbit": 1,
        "MType": 4,
        "FCntDn": 3900,
        "payload_hex": "901429c204282705",
        "mic_hex": "3af4037a",
        "Lrcid": "00000201",
        "LrrRSSI": -113.000000,
        "LrrSNR": -11.000000,
        "LrrESP": -124.331955,
        "SpFact": 8,
        "SubBand": "G1",
        "Channel": "LC1",
        "DevLrrCnt": 1,
        "Lrrid": "FF0109A4",
        "Late": 0,
        "LrrLAT": 60.242538,
        "LrrLON": 25.211100,
        "Lrrs": {
            "Lrr": [
                {
                    "Lrrid": "FF0109A4",
                    "Chain": 0,
                    "LrrRSSI": -113.000000,
                    "LrrSNR": -11.000000,
                    "LrrESP": -124.331955
                }
            ]
        },
        "CustomerID": "100002581",
        "CustomerData": {
            "alr":{
                "pro":"mcf88/lw12terwp",
                "ver":"1"
            }
        },
        "ModelCfg": "0",
        "DevAddr": "E00324CA",
        "TxPower": 14.000000,
        "NbTrans": 1,
        "Frequency": 868.1,
        "DynamicClass": "A"
    }
}'
