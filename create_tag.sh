tag_name=$1
tag_description=$2
if [ -z $tag_name ]
then
    echo Missing tag_name as first argument
    exit 1
fi
if [ -z "$tag_description" ]
then
    echo Missing tag_description as second argument
    exit 1
fi
echo $tag_name
git tag -a "$tag_name" -m "$tag_description"
git push origin "$tag_name"
