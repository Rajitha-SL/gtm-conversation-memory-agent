-- CreateTable
CREATE TABLE "CallSummary" (
    "id" TEXT NOT NULL,
    "callId" TEXT NOT NULL,
    "rawTranscript" TEXT NOT NULL,
    "aiAnalysisPass" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "CallSummary_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "CallSummary_callId_key" ON "CallSummary"("callId");
