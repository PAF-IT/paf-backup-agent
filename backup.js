import * as fs from 'node:fs'
import process from 'node:process'
import { readFile } from 'node:fs/promises'
import {
    S3Client,
    PutObjectCommand,
    ListObjectsV2Command,
    DeleteObjectCommand,
    GetObjectCommand
} from '@aws-sdk/client-s3'
import yaml from 'js-yaml'
import { DateTime } from 'luxon'
import mysqldump from 'mysqldump'


export const main = async () => {
    const obj_host = process.env.UPCLOUD_OBJ_HOST
    const config_file = process.env.IMAGE_TAG + '.yaml'
    let s3Client
    let mysql_host, mysql_port, mysql_databases
    let obj_bucket

    try {
        s3Client = new S3Client({
            region: 'eu-west-1',
            endpoint: obj_host,
            forcePathStyle: true
        })
        console.log(`Connected to ${obj_host}`)

        let response = await s3Client.send(
            new GetObjectCommand({
                Bucket: 'config',
                Key: config_file,
            })
        )

        // const config = yaml.load(fs.readFileSync('./paf-admin-backup-agent-dev.yaml'))
        const config = yaml.load(await response.Body.transformToString())
        console.log(JSON.stringify(config))
        
        mysql_host = config.mysql.host
        mysql_port = config.mysql.port
        mysql_databases = config.mysql.databases
        obj_bucket = config.objectStore.bucket
    } catch (e) {
        console.error(e)
        process.exitCode = 1
    }

    if (process.exitCode === 1 || !mysql_host || !mysql_port || ! mysql_databases) {
        console.error('Couldn\'t parse config from ENV, exiting.')
    } else {
        try {

            // Backup all databases
            await Promise.all(mysql_databases.map(async (db) => {
                const dumpfile = db.name + '_' + DateTime.now().toISO() + '.sql'
                await mysqldump({
                    connection: {
                        host: mysql_host,
                        user: db.user,
                        password: db.password,
                        database: db.name,
                    },
                    dumpToFile: dumpfile
                })
                console.log(`Dumped ${db.name} to ${dumpfile}`)

                await s3Client.send(new PutObjectCommand({
                        Bucket: obj_bucket,
                        Key: dumpfile,
                        Body: await readFile(dumpfile, 'utf8')
                }))
                console.log(`Successfully sent ${dumpfile} to ${obj_bucket} on ${obj_host}`)
            }))

            // Clean up (delete) all SQL files
            fs.readdir("./", (err, files) => {
                if (err)
                    console.error(`Could not read directory`, err)

                files.forEach((file) => {
                    if (file.toLowerCase().endsWith('.sql')) {
                        fs.unlinkSync(file)
                        console.log(`Deleted ${file} (locally)`)
                    }
                })
            })

            // Pruning of backups (older than 2 weeks)
            try {
                const response = await s3Client.send(new ListObjectsV2Command({
                    Bucket: obj_bucket,
                    MaxKeys: 1000
                }))
                await Promise.all(response.Contents.map(async (obj) => {
                    if (DateTime.fromJSDate(obj.LastModified).plus({days:14}) < DateTime.now()) {
                        await s3Client.send(new DeleteObjectCommand({
                            Bucket: obj_bucket,
                            Key: obj.Key,
                        }))
                        console.log(`Pruned ${obj.Key} (from bucket ${obj_bucket})`)
                    }
                }))
            } catch (e) {
                console.error(e)
            }
        } catch (e) {
            console.error(e)
        }
    }
}

await main()