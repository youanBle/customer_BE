const express = require('express');
const cors = require('cors')
const fs = require("fs");
var rp = require('request-promise');
const app = express();
app.use(cors())


const processCsvFile = (data, row) => {
    const dataString = data.toString();
    const arr = [];
    const rows = dataString.split("\r\n");
    rows.forEach((row) => {
        arr.push(row.split(','));
    })
    const slice = arr.slice(1, row + 1).map((value) => {
        return {
            storeId: value[0],
            customerId: value[1],
            postalCode: value[2],
            totalVisit: value[3],
            dollarSpend: value[4],
            productType: value[5],
        }
    });
    return slice;
}

const processPrizm = (data) => {
    if (data.format === 'multi') {
        return data.data[0].prizm_id;
    } else if (data.format === 'unique') {
        return data.data;
    } else if (data.format === 'non_residential_zoning') {
        return null;
    } else {
        return Math.floor(Math.random() * 68)
    }
}

const isPostalCodeValid = (str) => {
    const reqForPostal = /^[A-Za-z]\d[A-Za-z]\d[A-Za-z]\d$/
    return reqForPostal.test(str);
}


app.get('/custom_store_visiting/preview', (req, res) => {
    fs.readFile('CodingInterviewTestCustomerFileCSV_200.csv', (err, data) => {
        if (err) {
            console.log(err);
            return;
        }
        const output = processCsvFile(data, 6);
        res.send({ data: output });
    })
});



app.get('/custom_store_visiting/all', (req, res) => {
    const basicUrl = "https://prizm.environicsanalytics.com/api/pcode/get_segment"
    fs.readFile('CodingInterviewTestCustomerFileCSV_200.csv', (err, data) => {
        if (err) {
            console.log(err);
            return;
        }
        const customerVisiting = processCsvFile(data, 50);
        const validPostCode = {};

        //It will cost too much time if simply fetching PRIZM CODE for each record.
        //Instead, I picked up all unique postal codes and fetch the PRIZM for them, which means I don't need to send request for duplicate postal codes.
        customerVisiting.forEach((record) => {
            const pc = record.postalCode
            const postalCodeValid = isPostalCodeValid(pc);
            if (postalCodeValid && !(pc in validPostCode)) {
                validPostCode[pc] = true
            }
        })
        const validPostalCodeArr = Object.keys(validPostCode)
        const promiseArr = [];

        for (let i = 0; i < validPostalCodeArr.length; i++) {
            const postalCode = validPostalCodeArr[i]
            const url = `${basicUrl}?postal_code=${postalCode}`
            const options = {
                method: 'GET',
                url: url,
            }
            const promise = new Promise((resolve, rejects) => { 
                rp(options).then((value) => {
                    const processedResponse = JSON.parse(value);
                    const prizm = processPrizm(processedResponse)
                    resolve([postalCode, prizm])
                })

            })
            promiseArr.push(promise)
        }
        
        Promise.all(promiseArr).then(data => {
            const validMap = {}
            data.forEach((pair) => {
                if (pair[0]) {
                    validMap[pair[0]] = pair[1]
                }
            })
            const processedData = customerVisiting.map((record) => {
                if(record.postalCode in validMap){
                    return { ...record, prizm: validMap[record.postalCode] }
                }else{
                    return { ...record, prizm: -1 }
                }
                
            })
            res.send(processedData);
        })

    })

})

app.listen(80, () => {
    console.log('server starts at http://127.0.0.1')
});