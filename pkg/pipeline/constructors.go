package pipeline

// NewProcessStage constructs a ProcessStage.
func NewProcessStage() *ProcessStage { return &ProcessStage{} }

// NewEnrichStage constructs an EnrichStage.
func NewEnrichStage() *EnrichStage { return &EnrichStage{} }

// NewAnalyzeStage constructs an AnalyzeStage.
func NewAnalyzeStage() *AnalyzeStage { return &AnalyzeStage{} }

// NewSynthesizeStage constructs a SynthesizeStage.
func NewSynthesizeStage() *SynthesizeStage { return &SynthesizeStage{} }

// NewDeliverStage constructs a DeliverStage.
func NewDeliverStage() *DeliverStage { return &DeliverStage{} }
