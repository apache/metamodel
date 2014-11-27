## Thank you!

Before anything else, thank you. Thank you for taking some of your precious time helping this project move forward.

This guide will help you get started with Apache MetaModel's development environment. You'll also find the set of rules you're expected to follow in order to submit improvements and fixes to Apache MetaModel.

### Starter issues to work on

If you're looking for a relevant issue to work on, we suggest you to do the following:

* Join the @dev mailing list
 * To subscribe, send a mail to dev-subscribe@metamodel.apache.org
 * Afterwards communicate to the list via dev@metamodel.apache.org
* Check JIRA for the [issues with the 'starter' label](https://issues.apache.org/jira/issues/?jql=project%20%3D%20METAMODEL%20AND%20labels%20%3D%20starter).
* If you have something else on your mind, make yourself heard on the @dev mailing list.

### Build the code

Fork and clone the repository:

```
> git clone https://git-wip-us.apache.org/repos/asf/metamodel.git MetaModel
```

Try your first build:

```
> cd MetaModel
> mvn clean install
```

### Report issues and ideas

Please use our [JIRA system](https://issues.apache.org/jira/browse/METAMODEL) to report issues and ideas that you might work on. This is a great way to bounce ideas and get input to your contributions.

### Submitting your patch

When submitting your patch, keep in mind that it should be easy for others to review. For this reason we have two preferred ways to submit a patch - you can select the approach that fits you the best:

 * Use "git diff" to make a .patch file and submit it via our [Review Board](https://reviews.apache.org/groups/metamodel/).
 * Fork our [GitHub mirror](https://github.com/apache/metamodel) and post a [Pull Request](https://github.com/apache/metamodel/pulls).

### About tests

Your patches receive extra points if there's a good set of unittests with it. It's important to ensure that any non-trivial part of our project is well covered with tests.

By default the build will include all self-contained tests, including some quick integration tests to embedded databases like H2, Derby, HSQLDB etc.

Some tests have external dependencies to running servers etc. Those tests can be configured by adding a file called **metamodel-integrationtest-configuration.properties** to your local user's home folder. You can take a look at the [example-metamodel-integrationtest-configuration.properties file](https://raw.githubusercontent.com/apache/metamodel/master/example-metamodel-integrationtest-configuration.properties) to see which properties are available in order to configure the external server connections.

### Coding guidelines

If you plan on submitting code, read this carefully. Please note it is not yet complete.

We stick to the [Google Java Style Guide](http://google-styleguide.googlecode.com/svn/trunk/javaguide.html), with a few modifications/exceptions:

* We often prefix instance variables with an underscore (_). This to easily distinguish between method local and instance variables, as well as avoiding the overuse of the 'this' keyword in e.g. setter methods.
* We format indentation using spaces, not tabs. We use 4 spaces for each indentation.
* We format line wrapping using a desired max line length of 120 characters.