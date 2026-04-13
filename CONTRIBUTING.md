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

2.  **Install Git LFS**
    Some binary assets in this repo (e.g. `mirobody/res/*.bin`) are stored via [Git LFS](https://git-lfs.com). You must install it **before** cloning, otherwise you'll get tiny pointer files instead of the real content.
    ```bash
    # macOS:    brew install git-lfs
    # Ubuntu:   sudo apt install git-lfs
    # Then once per machine:
    git lfs install
    ```
    > **Note for external contributors:** GitHub LFS bandwidth/storage is billed against the *upstream* repository's quota, and forks do not inherit that quota. If your PR adds or modifies LFS-tracked files, the push from your fork may fail. In that case, please open an issue first so a maintainer can help land the binary asset.

3.  **Clone Your Fork**
    ```bash
    git clone https://github.com/YOUR_USERNAME/mirobody.git
    cd mirobody
    ```

4.  **Create a Branch**
    Create a new branch for your feature or fix:
    ```bash
    git checkout -b feature/my-new-feature
    # or
    git checkout -b fix/bug-fix-name
    ```

5.  **Make Changes**
    - Follow the existing code style.
    - Write clear and concise commit messages.

6.  **Test Your Changes**
    Ensure your changes don't break existing functionality. Run the deployment script locally to verify:
    ```bash
    ./deploy.sh
    ```

7.  **Push and Pull Request**
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
