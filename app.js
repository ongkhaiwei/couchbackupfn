const AWS = require('ibm-cos-sdk');
var stream = require('stream');
const couchbackup = require('@cloudant/couchbackup');
const moment = require('moment');

function couchBackupAction(params) {

    const bucket = params.bucket;
    const i = params.key.lastIndexOf('.');
    const key = (i < 0) ? params.key + '-' + moment().format('YYYYMMDD-hhmmss'): params.key.substring(0, i) + '-' + moment().format('YYYYMMDD-hhmmss') + params.key.substring(i);
    const cloudant_url = params.cloudant_url;
    const config = params.config;
    const s3 = new AWS.S3(config);

    return new Promise((resolve, reject) => {
        
        console.log('Setting up S3 upload to ' + bucket + '/' + key);

        // A pass through stream that has couchbackup's output
        // written to it and it then read by the S3 upload client.
        // It has a 10MB internal buffer.
        const streamToUpload = new stream.PassThrough({
            highWaterMark: 10485760
        });

        // Set up S3 upload.
        const params = {
            Bucket: bucket,
            Key: key,
            Body: streamToUpload
        };
        s3.upload(params, function (err, data) {
            console.log('S3 upload done');
            if (err) {
                console.log(err);
                reject(new Error('S3 upload failed'));
                return;
            }
            console.log('S3 upload succeeded');
            console.log(data);
            resolve();
        }).httpUploadProgress = (progress) => {
            console.log('S3 upload progress: ' + progress);
        };

        console.log('Starting streaming data from ' + cloudant_url);
        couchbackup.backup(
            cloudant_url,
            streamToUpload,
            (err, obj) => {
                if (err) {
                    console.log(err);
                    reject(new Error('CouchBackup failed with an error'));
                    return;
                }
                console.log('Download from ' + cloudant_url + ' complete.');
                streamToUpload.end(); // must call end() to complete S3 upload.
                // resolve() is called by the S3 upload
            }
        );
    });
}

exports.main = couchBackupAction;