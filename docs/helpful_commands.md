# Helpful commands for DP

## Table of contents

1. [Git](#git)
2. [Python](#python)

## Git

1. clone:
```bash
git clone <repo_name>
# Without .git!!!
```

2. merge:
```bash
#If u got some changes:
git stash

#Create branch
git switch -c <branch_name>

#Pop stash if u got changes!
git stash pop

#Actions...
git add <files>
git commit -m '...'
git push -u origin <branch_name>

#Go to test
git switch test
git pull

#Merge
git merge <branch_name>
git push -u origin test

#If test`s is OK - merge test to main
git switch main
git pull
git merge test
git push -u origin main
```

3. Work process:
```bash
#Pull all changes on local repo:
git pull
#Create\change branch
git switch -c <branch_name>
#Commit and merge all changes before u gone!
#All changes must be merged in test/ branch and then tested
#If test`s is OK - push to main
#Use understandable branch names:
# test/<type of change>/<changes area>/<changes essence>
```

## Python