package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "generate":
		if err := runGenerate(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "generate:service":
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "Error: service name required")
			fmt.Fprintln(os.Stderr, "Usage: protobus generate:service <package.ServiceName>")
			os.Exit(1)
		}
		if err := runGenerateService(os.Args[2]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "init":
		printInit()
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Protobus CLI - Code generation tools")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  protobus generate              Generate types from .proto files")
	fmt.Println("  protobus generate:service NAME Generate service stub and client")
	fmt.Println("  protobus init                  Show project setup instructions")
}

func printInit() {
	fmt.Println("Protobus Go Project Setup")
	fmt.Println("=" + strings.Repeat("=", 39))
	fmt.Println()
	fmt.Println("1. Create your proto directory:")
	fmt.Println("   mkdir -p proto")
	fmt.Println()
	fmt.Println("2. Create your .proto files in the proto directory")
	fmt.Println()
	fmt.Println("3. Generate Go types using protoc:")
	fmt.Println("   protoc --go_out=. --go_opt=paths=source_relative proto/*.proto")
	fmt.Println()
	fmt.Println("4. Generate service stubs and clients:")
	fmt.Println("   protobus generate:service package.ServiceName")
	fmt.Println()
	fmt.Println("5. Implement your service methods")
	fmt.Println()
	fmt.Println("6. Run your service:")
	fmt.Println("   go run ./cmd/myservice")
}

func runGenerate() error {
	protoDir := "proto"
	if len(os.Args) > 2 {
		protoDir = os.Args[2]
	}

	files, err := filepath.Glob(filepath.Join(protoDir, "*.proto"))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		fmt.Printf("No .proto files found in %s\n", protoDir)
		return nil
	}

	fmt.Printf("Found %d proto file(s)\n", len(files))

	for _, file := range files {
		services, err := extractServices(file)
		if err != nil {
			fmt.Printf("Warning: failed to parse %s: %v\n", file, err)
			continue
		}

		for _, svc := range services {
			fmt.Printf("  Found service: %s\n", svc.FullName)
		}
	}

	fmt.Println()
	fmt.Println("Run 'protoc --go_out=. proto/*.proto' to generate Go types")
	fmt.Println("Run 'protobus generate:service <name>' to generate service stubs")

	return nil
}

type serviceInfo struct {
	Package  string
	Name     string
	FullName string
	Methods  []methodInfo
}

type methodInfo struct {
	Name     string
	Request  string
	Response string
}

func extractServices(protoFile string) ([]serviceInfo, error) {
	file, err := os.Open(protoFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var services []serviceInfo
	var currentPackage string

	packageRe := regexp.MustCompile(`package\s+([a-zA-Z0-9_.]+)\s*;`)
	serviceRe := regexp.MustCompile(`service\s+([a-zA-Z0-9_]+)\s*\{`)
	rpcRe := regexp.MustCompile(`rpc\s+(\w+)\s*\(\s*(\w+)\s*\)\s*returns\s*\(\s*(\w+)\s*\)`)

	scanner := bufio.NewScanner(file)
	var inService *serviceInfo

	for scanner.Scan() {
		line := scanner.Text()

		if match := packageRe.FindStringSubmatch(line); match != nil {
			currentPackage = match[1]
		}

		if match := serviceRe.FindStringSubmatch(line); match != nil {
			fullName := match[1]
			if currentPackage != "" {
				fullName = currentPackage + "." + match[1]
			}
			inService = &serviceInfo{
				Package:  currentPackage,
				Name:     match[1],
				FullName: fullName,
				Methods:  make([]methodInfo, 0),
			}
		}

		if inService != nil {
			if match := rpcRe.FindStringSubmatch(line); match != nil {
				inService.Methods = append(inService.Methods, methodInfo{
					Name:     match[1],
					Request:  match[2],
					Response: match[3],
				})
			}

			if strings.Contains(line, "}") && !strings.Contains(line, "rpc") {
				services = append(services, *inService)
				inService = nil
			}
		}
	}

	return services, scanner.Err()
}

func runGenerateService(serviceName string) error {
	parts := strings.Split(serviceName, ".")
	if len(parts) < 2 {
		return fmt.Errorf("invalid service name format, expected 'package.ServiceName'")
	}

	packageName := strings.Join(parts[:len(parts)-1], ".")
	className := parts[len(parts)-1]

	// Try to find and parse the proto file
	protoFile := filepath.Join("proto", parts[0]+".proto")
	var methods []methodInfo

	if services, err := extractServices(protoFile); err == nil {
		for _, svc := range services {
			if svc.FullName == serviceName || svc.Name == className {
				methods = svc.Methods
				break
			}
		}
	}

	// Generate service implementation
	serviceCode := generateServiceCode(serviceName, packageName, className, methods)

	// Generate client code
	clientCode := generateClientCode(serviceName, packageName, className, methods)

	// Write files
	outputDir := "services"
	os.MkdirAll(outputDir, 0755)

	serviceFile := filepath.Join(outputDir, toSnakeCase(className)+"_service.go")
	clientFile := filepath.Join(outputDir, toSnakeCase(className)+"_client.go")

	if _, err := os.Stat(serviceFile); err == nil {
		fmt.Printf("Warning: %s already exists, skipping\n", serviceFile)
	} else {
		if err := os.WriteFile(serviceFile, []byte(serviceCode), 0644); err != nil {
			return fmt.Errorf("failed to write service file: %w", err)
		}
		fmt.Printf("Generated: %s\n", serviceFile)
	}

	if _, err := os.Stat(clientFile); err == nil {
		fmt.Printf("Warning: %s already exists, skipping\n", clientFile)
	} else {
		if err := os.WriteFile(clientFile, []byte(clientCode), 0644); err != nil {
			return fmt.Errorf("failed to write client file: %w", err)
		}
		fmt.Printf("Generated: %s\n", clientFile)
	}

	return nil
}

func generateServiceCode(serviceName, packageName, className string, methods []methodInfo) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("// Code generated by protobus. DO NOT EDIT.\n"))
	sb.WriteString(fmt.Sprintf("// Service: %s\n\n", serviceName))
	sb.WriteString("package services\n\n")
	sb.WriteString("import (\n")
	sb.WriteString("\t\"context\"\n\n")
	sb.WriteString("\t\"github.com/ArielLaub/protobus-go\"\n")
	sb.WriteString(")\n\n")

	// Service struct
	sb.WriteString(fmt.Sprintf("// %sService implements %s.\n", className, serviceName))
	sb.WriteString(fmt.Sprintf("type %sService struct {\n", className))
	sb.WriteString("\t*protobus.RunnableService\n")
	sb.WriteString("}\n\n")

	// Constructor
	sb.WriteString(fmt.Sprintf("// New%sService creates a new %sService.\n", className, className))
	sb.WriteString(fmt.Sprintf("func New%sService(ctx *protobus.Context, options *protobus.ServiceOptions) *%sService {\n", className, className))
	sb.WriteString(fmt.Sprintf("\ts := &%sService{\n", className))
	sb.WriteString(fmt.Sprintf("\t\tRunnableService: protobus.NewRunnableService(ctx, \"%s\", options),\n", serviceName))
	sb.WriteString("\t}\n")
	sb.WriteString("\ts.RegisterHandlers(s)\n")
	sb.WriteString("\treturn s\n")
	sb.WriteString("}\n\n")

	// Method stubs
	if len(methods) > 0 {
		for _, m := range methods {
			methodName := m.Name
			sb.WriteString(fmt.Sprintf("// %s handles the %s RPC call.\n", methodName, m.Name))
			sb.WriteString(fmt.Sprintf("func (s *%sService) %s(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {\n", className, methodName))
			sb.WriteString(fmt.Sprintf("\t// TODO: Implement %s\n", methodName))
			sb.WriteString(fmt.Sprintf("\treturn nil, protobus.NewHandledError(\"%s not implemented\", \"NOT_IMPLEMENTED\")\n", methodName))
			sb.WriteString("}\n\n")
		}
	} else {
		sb.WriteString("// TODO: Add your RPC method handlers here.\n")
		sb.WriteString("// Example:\n")
		sb.WriteString("// func (s *" + className + "Service) MyMethod(ctx context.Context, data map[string]interface{}, actor, correlationID string) (map[string]interface{}, error) {\n")
		sb.WriteString("//     return map[string]interface{}{\"result\": \"success\"}, nil\n")
		sb.WriteString("// }\n\n")
	}

	return sb.String()
}

func generateClientCode(serviceName, packageName, className string, methods []methodInfo) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("// Code generated by protobus. DO NOT EDIT.\n"))
	sb.WriteString(fmt.Sprintf("// Client for: %s\n\n", serviceName))
	sb.WriteString("package services\n\n")
	sb.WriteString("import (\n")
	sb.WriteString("\t\"context\"\n\n")
	sb.WriteString("\t\"github.com/ArielLaub/protobus-go\"\n")
	sb.WriteString(")\n\n")

	// Interface
	sb.WriteString(fmt.Sprintf("// %sClient is the interface for %s.\n", className, serviceName))
	sb.WriteString(fmt.Sprintf("type %sClient interface {\n", className))
	if len(methods) > 0 {
		for _, m := range methods {
			sb.WriteString(fmt.Sprintf("\t%s(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error)\n", m.Name))
		}
	} else {
		sb.WriteString("\t// Add your method signatures here\n")
	}
	sb.WriteString("}\n\n")

	// Implementation struct
	lowerClassName := strings.ToLower(className[:1]) + className[1:]
	sb.WriteString(fmt.Sprintf("// %sClient is the implementation of %sClient.\n", lowerClassName, className))
	sb.WriteString(fmt.Sprintf("type %sClient struct {\n", lowerClassName))
	sb.WriteString("\tproxy *protobus.ServiceProxy\n")
	sb.WriteString("}\n\n")

	// Constructor
	sb.WriteString(fmt.Sprintf("// New%sClient creates a new %sClient.\n", className, className))
	sb.WriteString(fmt.Sprintf("func New%sClient(ctx *protobus.Context) (%sClient, error) {\n", className, className))
	sb.WriteString(fmt.Sprintf("\tproxy := protobus.NewServiceProxy(ctx, \"%s\")\n", serviceName))
	sb.WriteString("\tif err := proxy.Init(); err != nil {\n")
	sb.WriteString("\t\treturn nil, err\n")
	sb.WriteString("\t}\n")
	sb.WriteString(fmt.Sprintf("\treturn &%sClient{proxy: proxy}, nil\n", lowerClassName))
	sb.WriteString("}\n\n")

	// Method implementations
	if len(methods) > 0 {
		for _, m := range methods {
			sb.WriteString(fmt.Sprintf("// %s calls the %s RPC method.\n", m.Name, m.Name))
			sb.WriteString(fmt.Sprintf("func (c *%sClient) %s(ctx context.Context, data map[string]interface{}) (map[string]interface{}, error) {\n", lowerClassName, m.Name))
			sb.WriteString("\tvar result map[string]interface{}\n")
			sb.WriteString(fmt.Sprintf("\terr := c.proxy.Call(ctx, \"%s\", data, &result)\n", m.Name))
			sb.WriteString("\treturn result, err\n")
			sb.WriteString("}\n\n")
		}
	}

	return sb.String()
}

func toSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteRune('_')
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}
