# Contributing

Contributions are very much encouraged, and we greatly appreciate the time and effort people make to help maintain and support our tools. Every contribution helps, so please don't be shy—we don't bite.

You can contribute to the development of our software in a number of different ways:

## Reporting bug fixes

Bugs are annoying, and reporting them helps us to fix issues quickly.

Bugs can be reported using the issue section on [GitHub](https://github.com/cgat-developers/cgat-core/issues).

When reporting issues, please include:

- Steps in your code/command that led to the bug so it can be reproduced.
- The error message from the log output.
- Any other helpful information, such as the system/cluster engine or version details.

## Proposing a new feature/enhancement

If you wish to contribute a new feature to the CGAT-core repository, the best way is to raise this as an issue and label it as an enhancement on [GitHub](https://github.com/cgat-developers/cgat-core/issues).

When proposing a new feature, please:

- Explain how your enhancement will work.
- Describe, as best as you can, how you plan to implement it.
- If you don't feel you have the necessary skills to implement this on your own, please mention it—we'll try our best to help (or even implement it for you). However, please note that this is community-developed software, and our volunteers have other jobs, so we may not be able to work as quickly as you might hope.

## Pull Request Guidelines

Why not contribute to our project? It's a great way of making the project better, and your help is always welcome. We follow the fork/pull request [model](https://guides.github.com/activities/forking/). To update our documentation, fix bugs, or add enhancements, you'll need to create a pull request through GitHub.

To create a pull request, follow these steps:

1. Create a GitHub account.
2. Create a personal fork of the project on GitHub.
3. Clone the fork onto your local machine. Your remote repo on GitHub is called `origin`.
4. Add the original repository as a remote called `upstream`.
5. If you made the fork a while ago, make sure you run `git pull upstream` to keep your repository up to date.
6. Create a new branch to work on! We usually name our branches with capital initials followed by a dash and something unique. For example: `git checkout -b AC-new_doc`.
7. Implement your fix/enhancement and make sure your code is effectively documented.
8. Our code has tests, which are run when a pull request is submitted. You can also run the tests beforehand—many of them are in the `tests/` directory. To run all tests, use `pytest --pep8 tests`.
9. Add or modify the documentation in the `docs/` directory.
10. Squash all of your commits into a single commit using Git's [interactive rebase](https://help.github.com/articles/about-git-rebase/).
11. Push your branch to your fork on GitHub: `git push origin`.
12. From your fork on GitHub, open a pull request in the correct branch.
13. Someone will review your changes, and they may suggest modifications or approve them.
14. Once the pull request is approved and merged, pull the changes from `upstream` to your local repo and delete your branch.

> **Note**: Always write your commit messages in the present tense. Your commit messages should describe what the commit does to the code, not what you did to the code.