import os

def find_project_root(current_path):
    # Traverse up until you find a directory with a 'config' directory
    while not os.path.exists(os.path.join(current_path, 'config')):
        parent = os.path.dirname(current_path)
        if parent == current_path:
            # The 'config' directory isn't found; you've reached the filesystem root
            raise FileNotFoundError('Could not find the project root with a "config" directory')
        current_path = parent
    return current_path

# Determine the project root when the package is first imported
project_root = find_project_root(os.path.abspath(os.path.dirname(__file__)))

# You can then use 'project_root' within the package to construct paths to resources
