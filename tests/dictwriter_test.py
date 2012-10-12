import limnpy, datetime, pprint
writer = limnpy.DictWriter('evan_test', "Evan's Test", keys=['date', 'x', 'y'])
rows = [{'date' : datetime.date(2012, 9, 1), 'x' : 1, 'y' : 2},
        {'date' : datetime.date(2012, 10, 1), 'x' : 7, 'y' : 9},]
writer.write_rows(rows)
print open('./datasources/evan_test.yaml').read().strip()
print open('./datafiles/evan_test.csv').read().strip()
