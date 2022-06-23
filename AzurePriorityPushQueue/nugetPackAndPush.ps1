param([Parameter(Mandatory=$true)] $ApiKey)

$xml = [xml](gc .\AzurePriorityPushQueue.csproj)
$packageId = $xml.Project.PropertyGroup[1].PackageId
$version = $xml.Project.PropertyGroup[1].Version
$target = "$packageId.$version.nupkg"

del $target -ErrorAction Ignore
dotnet pack --configuration Release --output .

dotnet nuget push $target --api-key $ApiKey --source https://api.nuget.org/v3/index.json