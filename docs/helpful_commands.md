# Helpful commands for DP
## Table of contents

1. [Git](#git)
2. [Python](#python)

### Git

1. clone: 
```bash
git clone <repo_name> 
(Without .git!!!)
```

2. merge:
```bash
#If u got some changes:
git stash
#After all ops:
git stash pop #For getting current unsaved head
#in main or any other branch
#Create branch
git branch <branch_name>
git switch <branch_name>
#or
git switch -c <branch_name>
#Pop stash if u got changes!
#Actions...
git commit -m '...'
git push -u origin <branch_name>
#Go to main
git checkout test
#Merge
git merge <branch_name>
#Push
git push -u origin test
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
### Python