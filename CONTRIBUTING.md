# Contributing to Mirobody

Thank you for your interest in contributing to Mirobody! We welcome contributions from the community to help make this project better.

## 🤝 How to Contribute

### Reporting Bugs
If you find a bug, please create a new issue on GitHub. include:
- A clear title and description.
- Steps to reproduce the issue.
- Your environment details (OS, Docker version, etc.).

### Suggesting Features
We love new ideas! Please open an issue to discuss your feature idea before implementing it. This helps ensure your time is well spent and the feature aligns with the project's goals.

## 🛠️ Development Workflow

1.  **Fork the Repository**
    Click the "Fork" button on the top right of the repository page.

2.  **Clone Your Fork**
    ```bash
    git clone https://github.com/YOUR_USERNAME/mirobody.git
    cd mirobody
    ```

3.  **Create a Branch**
    Create a new branch for your feature or fix:
    ```bash
    git checkout -b feature/my-new-feature
    # or
    git checkout -b fix/bug-fix-name
    ```

4.  **Make Changes**
    - Follow the existing code style.
    - Write clear and concise commit messages.

5.  **Test Your Changes**
    Ensure your changes don't break existing functionality. Run the deployment script locally to verify:
    ```bash
    ./deploy.sh
    ```

6.  **Push and Pull Request**
    Push your branch to your fork:
    ```bash
    git push origin feature/my-new-feature
    ```
    Then, open a Pull Request (PR) against the `main` branch of the original repository.

## 📝 Coding Style

- **Python**: We follow PEP 8 guidelines.
- **Documentation**: Update README or other docs if you change how something works.
- **Commits**: Use descriptive commit messages.

## ⚖️ License
By contributing, you agree that your contributions will be licensed under the project's [LICENSE](./LICENSE).
