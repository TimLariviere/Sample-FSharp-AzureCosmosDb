// Learn more about F# at http://fsharp.org

open System
open System.IO
open Microsoft.Extensions.Configuration
open Microsoft.Azure.Documents.Client
open Microsoft.Azure.Documents
open Microsoft.Azure.Documents.Linq
open Microsoft.Azure.Graphs
open Microsoft.Azure.Graphs.Elements
open FSharp.Control

type AzureCosmosDbConfiguration =
    {
        Endpoint: string
        AuthKey: string
        DatabaseName: string
        GraphName: string
        OfferThroughput: int
    }

// Read configuration
let getConfiguration() =
    let configurationBuilder = new ConfigurationBuilder()
    configurationBuilder
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json")
        .Build()

let readAzureCosmosDbConfiguration (configuration: IConfigurationRoot) =
    {
        Endpoint = configuration.["AzureCosmosDb:Endpoint"]
        AuthKey = configuration.["AzureCosmosDb:AuthKey"]
        DatabaseName = configuration.["AzureCosmosDb:DatabaseName"]
        GraphName = configuration.["AzureCosmosDb:GraphName"]
        OfferThroughput = (configuration.["AzureCosmosDb:OfferThroughput"] |> int)
    }

// Initialize Azure Cosmos DB
let createClient endpoint (authKey: string) =
    new DocumentClient(
        new Uri(endpoint),
        authKey,
        ConnectionPolicy.Default
    )

let createDatabaseAsync databaseName (client: DocumentClient) =
    new Database (Id = databaseName)
    |> client.CreateDatabaseIfNotExistsAsync
    |> Async.AwaitTask
    |> Async.Ignore

let createGraphAsync databaseName graphName offerThroughput (client: DocumentClient) =
    client.CreateDocumentCollectionIfNotExistsAsync(
        UriFactory.CreateDatabaseUri(databaseName),
        new DocumentCollection (Id = graphName),
        new RequestOptions (OfferThroughput = Nullable<int>(offerThroughput))
    )
    |> Async.AwaitTask
    |> (fun asyncResult -> async {
        let! result = asyncResult
        return result.Resource
    })

// Run queries
let runGremlinQuery<'T> (dq: IDocumentQuery<'T>) = asyncSeq {
    while (dq.HasMoreResults) do
        let! items = dq.ExecuteNextAsync<'T>() |> Async.AwaitTask
        for item in items do
            yield item
}

let runQueryWithClient<'T> (client: DocumentClient) graph query =
    client.CreateGremlinQuery<'T>(graph, query)
    |> runGremlinQuery<'T>

// Extract properties from Vertex
let getProperty<'T> propertyName (vertex: Vertex) =
    vertex.GetVertexProperties(propertyName)
    |> Seq.head
    |> (fun vp -> vp.Value :?> 'T)

let printPersonVertex person =
    let firstName = getProperty<string> "firstName" person
    let lastName = getProperty<string> "lastName" person
    let age = getProperty<int64> "age" person

    printfn "%s %s (age %i)" firstName lastName age

// Main
let asyncMain() = async {
    // Initialize
    let configuration = getConfiguration() |> readAzureCosmosDbConfiguration
    let client = createClient configuration.Endpoint configuration.AuthKey

    do! createDatabaseAsync configuration.DatabaseName client
    
    let! graph = createGraphAsync
                    configuration.DatabaseName
                    configuration.GraphName
                    configuration.OfferThroughput
                    client

    // Prepare query runners
    let executeQuery = runQueryWithClient<obj> client graph >> AsyncSeq.iter ignore
    let getVertices = runQueryWithClient<Vertex> client graph >> AsyncSeq.toListAsync

    // Add data
    do! executeQuery "g.V().drop()" // Clear any existing data
    do! executeQuery "g.addV('person').property('id', 'thomas.1').property('firstName', 'Thomas').property('lastName', 'Andersen').property('age', 44)"
    do! executeQuery "g.addV('person').property('id', 'robin.1').property('firstName', 'Robin').property('lastName', 'Smith').property('age', 42)"
    do! executeQuery "g.addV('person').property('id', 'paul.1').property('firstName', 'Paul').property('lastName', 'Smith').property('age', 26)"
    do! executeQuery "g.V('thomas.1').addE('knows').to(g.V('robin.1'))"
    do! executeQuery "g.V('thomas.1').addE('knows').to(g.V('paul.1'))"

    // Query people that Thomas knows
    printfn "Thomas knows:"

    let! people = getVertices "g.V('thomas.1').out('knows')"
    do people |> List.iter printPersonVertex
}

[<EntryPoint>]
let main _ =
    asyncMain() |> Async.RunSynchronously
    0 // return an integer exit code
