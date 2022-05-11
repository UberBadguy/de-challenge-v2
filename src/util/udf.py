class UDFS():

    def getSeasonFromFilename(x):
        index = x.rindex('/') 
        outsubstr = x[index+1:]
        return outsubstr.replace('season-','').replace('_json.json','')
