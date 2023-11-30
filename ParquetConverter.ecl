IMPORT Std;
IMPORT PYTHON3 as PYTHON;
IMPORT lib_fileservices;
//Environment config
// apt-get update
// apt-get install python3-pip
// apt-get install python3-pandas
// sudo apt-get install cython
// pip3 install --upgrade cython
// apt-get install pip
// python3 -m pip instal "pyarrow=1.0.0"
// python3 -m pip instal "pyarrow=0.5.0"

//This code:
// 1. points to a raw Parquet file on the LZ
// 2. use Python to create a dataframe for this file
// 3. return it to ECL as a string with @@ as record terminator
// 4. use this string to declare a dataset
// 5. output the dataset to disk
// NOTE THAT THE FILE WASN'T SPRAYED AT ANY MOMENT
 
IMPORT Std;
IMPORT PYTHON3 as PYTHON;
 
fnDataFrameTest(STRING filename) := FUNCTION
    landingZoneDirectory := lib_fileservices.FileServices.GetDefaultDropZone() + '/';   
    STRING pyTest(STRING pyfilename) := EMBED(PYTHON)
        #importing libraries
        import re
        import pandas 
 
        #read the parquet 
        dataframe=pandas.read_parquet(pyfilename)
 
        #convert the df to csv and reformat it
        csv_string = dataframe.to_string(index=False)
        csv_string = re.sub('^\s+', '', csv_string, flags=re.MULTILINE)
        csv_string = re.sub('[ ]+', ',', csv_string)
        csv_string = re.sub('\n', '@@', csv_string)
 
        # #return the string
        return csv_string
    ENDEMBED;
    RETURN pyTest(landingZoneDirectory + filename);
END;
 
STRING getData := fnDataframeTest('iris.parquet');
splitRecords := STD.STR.SplitWords(getData, '@@');
myds := DATASET (splitRecords, {STRING line});
OUTPUT(myds,,'~parquetfile::test', OVERWRITE);
