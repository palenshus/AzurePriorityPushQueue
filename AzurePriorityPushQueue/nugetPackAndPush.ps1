param([Parameter(Mandatory=$true)] $ApiKey)

del *.nupkg
nuget pack .\AzurePriorityPushQueue.csproj
nuget push *.nupkg $ApiKey -Source https://www.nuget.org/api/v2/package