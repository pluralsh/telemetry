{{- /*
Renders the API types as JSON for the docs site (documentation/operator/api.json).
crd-ref-docs runs it with the markdown renderer, so type references are Markdown
links: local types link to "#<lowercased name>", known types to their docs.
*/ -}}
{{- define "gvList" -}}
{{- $groupVersions := list -}}
{{- range . -}}
{{- $gv := . -}}
{{- $types := list -}}
{{- range $gv.SortedTypes -}}
{{- if markdownShouldRenderType . -}}
{{- $type := . -}}
{{- $kind := "" -}}
{{- if $type.GVK -}}{{- $kind = $type.GVK.Kind -}}{{- end -}}
{{- $underlying := "" -}}
{{- if $type.IsAlias -}}{{- $underlying = markdownRenderTypeLink $type.UnderlyingType -}}{{- end -}}
{{- $fields := list -}}
{{- range $type.Members -}}
{{- $fieldRules := list -}}
{{- range (index .Markers "kubebuilder:validation:XValidation") -}}
{{- $fieldRules = append $fieldRules (dict "rule" .Rule "message" .Message) -}}
{{- end -}}
{{- $fields = append $fields (dict "name" .Name "type" (markdownRenderType .Type) "doc" .Doc "default" .Default "validation" (default (list) .Validation) "rules" $fieldRules) -}}
{{- end -}}
{{- $appearsIn := list -}}
{{- range $type.SortedReferences -}}
{{- $appearsIn = append $appearsIn .Name -}}
{{- end -}}
{{- $enumValues := list -}}
{{- range $type.EnumValues -}}
{{- $enumValues = append $enumValues (dict "name" .Name "doc" .Doc) -}}
{{- end -}}
{{- $rules := list -}}
{{- range (index $type.Markers "kubebuilder:validation:XValidation") -}}
{{- $rules = append $rules (dict "rule" .Rule "message" .Message) -}}
{{- end -}}
{{- $resource := dict -}}
{{- range (index $type.Markers "kubebuilder:resource") -}}
{{- $resource = dict "scope" (default "Namespaced" .Scope) "shortNames" (default (list) .ShortName) -}}
{{- end -}}
{{- $types = append $types (dict "name" $type.Name "kind" $kind "resource" $resource "doc" $type.Doc "underlying" $underlying "validation" (default (list) $type.Validation) "rules" $rules "fields" $fields "appearsIn" $appearsIn "enumValues" $enumValues) -}}
{{- end -}}
{{- end -}}
{{- $groupVersions = append $groupVersions (dict "groupVersion" $gv.GroupVersionString "doc" $gv.Doc "kinds" (default (list) $gv.SortedKinds) "types" $types) -}}
{{- end -}}
{{- toPrettyJson (dict "groupVersions" $groupVersions) }}
{{ end -}}
