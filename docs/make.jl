using Pkg

Pkg.activate(@__DIR__)
try
    Pkg.Registry.add("General")
catch err
    if !occursin("already exists", sprint(showerror, err))
        rethrow()
    end
end
Pkg.instantiate()

using Documenter

makedocs(
    sitename = "CirQuant",
    format = Documenter.HTML(
        prettyurls = get(ENV, "CI", "false") == "true",
    ),
    pages = [
        "Home" => "index.md",
        "Methodology" => "methodology.md",
        "Configuration Guide" => "configuration-guide.md",
        "Parameters Reference" => "parameters-reference.md",
        "Data Sources" => "data-sources.md",
        "Database Schema" => [
            "Raw" => "database-schema-raw.md",
            "Processed" => "database-schema-processed.md",
        ],
        "Roadmap" => "roadmap.md",
    ],
)

deploydocs(
    repo = "github.com/equicirco/cirquant.git",
    devbranch = "main",
)
