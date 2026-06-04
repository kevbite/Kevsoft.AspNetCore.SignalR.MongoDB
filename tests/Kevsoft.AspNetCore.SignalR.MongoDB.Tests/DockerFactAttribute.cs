using System.Diagnostics;

namespace Kevsoft.AspNetCore.SignalR.MongoDB.Tests;

public sealed class DockerFactAttribute : FactAttribute
{
    public DockerFactAttribute()
    {
        if (!IsDockerAvailable())
        {
            Skip = "Docker is not available; skipping real MongoDB integration test.";
        }
    }

    private static bool IsDockerAvailable()
    {
        try
        {
            using var process = Process.Start(new ProcessStartInfo
            {
                FileName = "/bin/sh",
                ArgumentList = { "-c", "docker info >/dev/null 2>&1" },
                RedirectStandardOutput = true,
                RedirectStandardError = true
            });

            return process != null &&
                process.WaitForExit(3000) &&
                process.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }
}
