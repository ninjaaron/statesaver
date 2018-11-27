#!/usr/bin/env python3
import statesaver

jstate = statesaver.JState('jcache')
try:
    print(jstate['foo'])
except KeyError:
    pass
with jstate:
    jstate['foo'] = 'bar'


# more later...
dbstate = statesaver.DBState('dbcache')

for i in statesaver.PlayQueue('loopy', range(10)):
    print(i)
    if i == 5:
        break
print('here')
for i in statesaver.PlayQueue('loopy', range(10)):
    print(i)
