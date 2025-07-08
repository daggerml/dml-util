# Contributing to dml-util

Thank you for your interest in contributing! We welcome contributions via pull requests and appreciate your help in improving this project.

## Reporting Issues

- Search [existing issues](https://github.com/daggerml/dml-util/issues) before submitting a new one.
- When reporting a bug, please include:
  - A clear, descriptive title.
  - Steps to reproduce the issue.
  - Expected and actual behavior.
  - Python version and operating system.
  - Relevant code snippets or error messages.

## How to Contribute Code

### **Want to dive in?**

1. Find an issue you'd like to tackle from GitHub [Issues](https://github.com/daggerml/dml-util/issues)
2. Check out the *Assignees* section on the issue tracker page. If nobody is already assigned, feel free to assign yourself. Otherwise message the assignee first to coordinate.
3. Create a branch for your work.
4. Clone the repository and set it up:
   ```bash
   git clone https://github.com/daggerml/dml-util.git
   ```
5. Make your changes in the new branch, following the coding standards below.
6. Add or update tests as needed.
7. Ensure all tests pass locally.
8. Commit and push changes to your branch.
9. Once your code is ready, rebase off of main and create a pull request. Tag a maintainer for review. DO NOT MERGE TO MAIN YOURSELF.

## Coding Standards

- Follow [PEP 8](https://pep8.org/) for Python code style.
- Use [numpy style docstrings](https://numpydoc.readthedocs.io/en/latest/format.html) for all public modules, classes, functions, and methods.
- Use double quotes for strings.
- Write clear, concise commit messages.
- Keep pull requests focused and minimal.

## Project Structure

- The library is split into front-end (user-facing) and back-end (adapters/runners) components.
- Tests are organized to mirror the `src/` structure:
  - `tests/unit/` matches the module layout in `src/`.
  - `tests/integration/` contains end-to-end and workflow tests.
  - `tests/assets/` contains reusable test DAGs/functions.
- Unit tests should be runnable even without DaggerML installed.

## Testing Guidelines

- Add or update tests for any new features or bug fixes.
- Use [pytest](https://pytest.org/) for running tests.
- Integration tests should cover real-world usage patterns, including:
  - Wrapping functions with `funkify`.
  - Using S3Store for artifact storage.
  - Running with different adapters/runners (local, conda, docker, batch, etc).
  - Error handling and logging.
- Run all tests locally before submitting a pull request.
- Ensure your code passes all tests and does not decrease code coverage.
- If your changes introduce new dependencies, please update `pyproject.toml`.

## Example: Wrapping a Function with Numpy Docstring

```python
from daggerml import Dml
from dml_util import funkify

@funkify
def add_numbers(dag):
    """
    Add numbers together.

    Parameters
    ----------
    dag : DmlDag
        The DAG context provided by DaggerML.

    Returns
    -------
    int
        The sum of the input numbers.
    """
    dag.result = sum(dag.argv[1:].value())
    return dag.result
```

## Resources

- [numpy docstring style guide](https://numpydoc.readthedocs.io/en/latest/format.html)
- [DaggerML python-lib](https://github.com/daggerml/python-lib)
- [DaggerML CLI](https://github.com/daggerml/daggerml-cli)

Thank you for helping make this project better!
