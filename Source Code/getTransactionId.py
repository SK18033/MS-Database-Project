from collections import defaultdict
import csv

fp1 = open("teams.csv", 'r')
reader = csv.reader(fp1)

#target = open("trans_output.txt", 'w')

d = defaultdict(list)

for tid in reader:
	fp2 = open("submissions_metadata.csv", 'r')
	reader2 = csv.reader(fp2)
	for teamid in reader2:
		if teamid[1] == tid[0]:
			d[tid[0]].append(teamid[0])

with open('opt.csv','wb') as out:
	writer = csv.writer(out)
	for k,v in d.items():
		writer.writerow([k,v])
		#writer.writerow(v)

fp1.close()
fp2.close()
	
