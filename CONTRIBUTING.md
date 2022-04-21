# Contribution Guide

Contributions of any kind are very welcome! If you would like to contribute something,  this document should help you.

DBPack is released under the Apache 2.0 license. Please using Github tracker for issues and merging pull requests.

Before we accept a non-trivial patch or pull request we will need you to sign the Contributor License Agreement. Signing the contributor’s agreement does not grant anyone commits rights to the main repository, but it does mean that we can accept your contributions, and you will get an author credit if we do. Active contributors might be asked to join the core team and given the ability to merge pull requests.

## Conventions

+ Before submit a pull request, you should run `goimports -w ${filepath}` and `golint ${filepath}` to format the style.
  
  With goland ide, select `Move all stdlib imports in a single group`, `Group stdlib imports`, `Move all imports in a single declaration` in Editor -> Code Style -> Go -> imports page.
  
+ Make sure all the github actions run through.

