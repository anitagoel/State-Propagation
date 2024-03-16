import os
import json
import s3fs
import boto3
import pickle

#{
#  "Records": [
#    {
#      "s3": {
#        "bucket": {
#          "name": "word-freq",
#          "arn": "arn:aws:s3:::word-freq"
#        },
#        "object": {
#          "dirPath" : "data" ,
#          "fileName": "input.txt",
#	       "startByte": 1,
#	       "readSize" : 60,
#	       "outFileName" : "output.txt"
#          "invokationNum" : 1    
#        }
#      }
#    }
#  ]
#}

s3 = s3fs.S3FileSystem(anon=False)

def lambda_handler(event, context):
    #print("Received event: " + str(event))

    counts = dict()
    bucket = "" 
    dirPath =  ""  
    fileName = ""
    startByte = 0
    readSize = 0
    outFileName =  ""
    invokationNum = 0
    bytesRead = ""

    for record in event['Records']:
        # Create some variables that make it easier to work with the data in the
        # event record.
        bucket = record['s3']['bucket']['name']
        dirPath =  record['s3']['object']['dirPath']
        fileName = record['s3']['object']['fileName']
        startByte = record['s3']['object']['startByte']
        readSize = record['s3']['object']['readSize']
        outFileName = record['s3']['object']['outFileName']
        invokationNum = record['s3']['object']['invokationNum']

        
    if startByte > 2:
        #counts.clear()
        newFilePath = os.path.join(bucket, dirPath, outFileName)
        f = s3.open(newFilePath,"rb")  
        print("Reading Dictionary from S3")
        counts = pickle.load(f)
        f.close()

        
    input_file = os.path.join(bucket, dirPath, fileName)
    print("This is invokationNum :", invokationNum, " startByte: ", startByte)

    fileSize = s3.size(input_file)
    #print("Total Size: ", fileSize)
        
    addCount = 0
    invokationNum = invokationNum +1

    with s3.open(input_file, 'r') as f:
        addCount = 0
        # find the next newline 
        f.seek(startByte+readSize - 1 ) # seek is initially at byte 0 and then moves forward the specified amount, so seek(5) points at the 6th byte
        bytesRead = f.read(1)
        if bytesRead != ' ' and bytesRead != '\n' and bytesRead != '\r' and bytesRead != '\t' :
            while (bytesRead != ' ' and bytesRead != '\n' and bytesRead != '\r' and bytesRead != '\t') and (startByte + readSize + addCount) < fileSize:
                #print("byteread:",bytesRead,":")
                addCount = addCount + 1
                f.seek(startByte + readSize + addCount - 1 )
                bytesRead = f.read(1)
        #print(" addCount :", addCount,", Final Read till:",startByte+readSize+addCount+1)
        f.seek(startByte-1) # seek is initially at byte 0 and then moves forward the specified amount, so seek(5) points at the 6th byte
        ##if bytesRead == '\n':
        ##    addCount = addCount + 1
        bytesRead = f.read( readSize + addCount-1)
        #print("Return Bytes: ", bytesRead)


    #print("----Now do our processing here-----")
    words = bytesRead.split()
    for word in words:
        if word in counts:
            counts[word] += 1
        else:
            counts[word] = 1

#        if (startByte + readSize + addCount) >= fileSize:
#            print("time to write? (startByte + readSize + addCount):",(startByte + readSize + addCount)," >= fileSize:", fileSize)
#            print("now write these in a new file")
#            newFile = outFileName + str(invokationNum)
#            newFilePath = os.path.join(bucket, dirPath, newFile)
#            fw = s3.open(newFilePath, "w")

#            for word in counts:
#                writestring = word + " : " + str(counts[word]) +  " \n"
#                fw.write(writestring)

#            fw.close()
#            print("now write these in a new file: done")

    # Now update the dictionary in the binary pickle file 
    newFilePath = os.path.join(bucket, dirPath, outFileName)
    f = s3.open(newFilePath,"wb")        
    pickle.dump(counts, f)
    f.close()
    
    
    #check if there is still data in file
    if (startByte + readSize + addCount  ) < fileSize:
        
        newStartByte = startByte + readSize + addCount 
        # Invoke Lambda recursively
        invokeLam = boto3.client("lambda", region_name="us-east-2")
        
        #payload = {"message": "Test"}
        #payload = "{\"Records\": [{ \"s3\": {\"bucket\": {\"name\": \"extsorting\", \"arn\": \"arn:aws:s3:::extsorting\"},\"object\": {\"key\": \""+ obj_sum.key +"\"}}}]}"
        payload = json.dumps({"Records":[{"s3":{"bucket":{"name":"extsorting","arn":"arn:aws:s3:::extsorting"},"object":{"dirPath": dirPath, "fileName": fileName, "startByte": newStartByte, "readSize" : readSize, "outFileName" : outFileName, "invokationNum" : invokationNum }}}]})
        
        #print("Recursive newFileSplit Function Getting Invoked with Pauload : ",payload)
        print("Next invokationNum :", invokationNum, " startByte: ", newStartByte)
        
        resp = invokeLam.invoke(FunctionName="wordfrequency", InvocationType = "Event", Payload = payload)
        print("wordfrequency Invoke : Done ")
        # ReadFileSegment((startByte + readSize + addCount+3), readSize, fileName, fileBytes, searchChar, fileNum)
       
    # Write counts in a new file if the file processing is done
    print("check - time to write? (startByte + readSize + addCount):",(startByte + readSize + addCount)," >= fileSize:", fileSize)
    if (startByte + readSize + addCount) >= fileSize:
        print("yes - time to write? (startByte + readSize + addCount):",(startByte + readSize + addCount)," >= fileSize:", fileSize)
        print("now write these in a new file")
        newFile = outFileName + "Final.txt"
        newFilePath = os.path.join(bucket, dirPath, newFile)
        fw = s3.open(newFilePath, "w")

        for word in counts:
            writestring = word + " : " + str(counts[word]) +  " \n"
            fw.write(writestring)

        fw.close()
        print("now write these in a new file: done")
