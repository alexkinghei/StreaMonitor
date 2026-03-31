#!/usr/bin/env sh

set -eu

usage() {
    cat <<'EOF'
Usage:
  ./quick-commit.sh
  ./quick-commit.sh "commit message"
  ./quick-commit.sh "commit message" --no-push

Examples:
  ./quick-commit.sh
  ./quick-commit.sh "fix stripchat cookie handling"
  ./quick-commit.sh "update docker config" --no-push
EOF
}

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Error: current directory is not a git repository." >&2
    exit 1
fi

push_after_commit=1
commit_message=""

for arg in "$@"; do
    case "$arg" in
        --no-push)
            push_after_commit=0
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            if [ -n "$commit_message" ]; then
                echo "Error: only one commit message is allowed." >&2
                usage >&2
                exit 1
            fi
            commit_message=$arg
            ;;
    esac
done

branch_name=$(git branch --show-current || true)

echo "Branch: ${branch_name:-DETACHED_HEAD}"
echo "Current changes:"
git status --short
echo

if [ -z "$commit_message" ]; then
    printf 'Commit message: '
    IFS= read -r commit_message
fi

if [ -z "$commit_message" ]; then
    echo "Error: commit message is required." >&2
    exit 1
fi

echo "Staging changes..."
git add -A

if git diff --cached --quiet; then
    echo "No staged changes to commit."
    exit 0
fi

echo "Creating commit on branch: ${branch_name:-DETACHED_HEAD}"
git commit -m "$commit_message"

if [ "$push_after_commit" -eq 1 ]; then
    echo "Pushing commit..."
    if [ -n "$branch_name" ]; then
        if git rev-parse --abbrev-ref --symbolic-full-name "@{u}" >/dev/null 2>&1; then
            git push
        else
            git push -u origin "$branch_name"
        fi
    else
        git push
    fi
fi

echo "Done."
